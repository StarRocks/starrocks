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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptDistributionPruner;
import com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;

import java.util.List;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator.SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES;

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

    /**
     * For input query expression, reset/clear pruned partitions and return new query expression to be pruned again.
     * @param optExpression: optExpression of input query
     * @return: a new query expression with pruned partitions cleared
     */
    public static OptExpression resetSelectedPartitions(OptExpression optExpression, boolean refreshTableMetadata) {
        return optExpression.getOp().accept(new SelectedPartitionCleanerVisitor(refreshTableMetadata), optExpression, null);
    }

    public static LogicalOlapScanOperator resetSelectedPartitions(LogicalOlapScanOperator olapScanOperator) {
        final LogicalOlapScanOperator.Builder mvScanBuilder = OperatorBuilderFactory.build(olapScanOperator);
        // reset original partition predicates to prune partitions/tablets again
        mvScanBuilder.withOperator(olapScanOperator)
                .setSelectedPartitionId(null)
                .setPrunedPartitionPredicates(Lists.newArrayList())
                .setSelectedTabletId(Lists.newArrayList());
        return mvScanBuilder.build();
    }

    private static class SelectedPartitionCleanerVisitor extends OptExpressionVisitor<OptExpression, Void> {
        private final boolean refreshTableMetadata;
        public SelectedPartitionCleanerVisitor(boolean refreshTableMetadata) {
            this.refreshTableMetadata = refreshTableMetadata;
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOperator = optExpression.getOp().cast();

            if (scanOperator instanceof LogicalOlapScanOperator) {
                // NOTE: need clear original partition predicates before,
                // original partition predicates if cannot be rewritten may contain wrong slot refs.
                // MV   : select c1, c3, c2 from test_base_part where c2 < 2000
                // Query: select c1, c3, c2 from test_base_part where c2 < 3000 and c3 < 3000
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) (scanOperator);
                LogicalScanOperator newOlapScanOperator = resetSelectedPartitions(olapScanOperator);
                return OptExpression.create(newOlapScanOperator);
            } else {
                try {
                    ScanOperatorPredicates operatorPredicates = scanOperator.getScanOperatorPredicates();
                    operatorPredicates.clear();
                } catch (AnalysisException e) {
                    // ignore
                }

                final LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scanOperator);
                // reset original partition predicates to prune partitions/tablets again
                builder.withOperator(scanOperator);
                if (refreshTableMetadata && scanOperator.getOpType() == OperatorType.LOGICAL_ICEBERG_SCAN) {
                    // refresh iceberg table's metadata
                    Table refBaseTable = scanOperator.getTable();
                    IcebergTable cachedIcebergTable = (IcebergTable) refBaseTable;
                    String catalogName = cachedIcebergTable.getCatalogName();
                    String dbName = cachedIcebergTable.getRemoteDbName();
                    TableName tableName = new TableName(catalogName, dbName, cachedIcebergTable.getName());
                    Table currentTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName).orElse(null);
                    if (currentTable == null) {
                        return null;
                    }
                    // Iceberg table's snapshot is cached in the mv's plan cache, need to reset it to get the latest snapshot
                    builder.setTable(currentTable);
                }
                LogicalScanOperator newScanOperator = builder.build();
                return OptExpression.create(newScanOperator);
            }
        }

        public OptExpression visit(OptExpression optExpression, Void context) {
            List<OptExpression> children = Lists.newArrayList();
            for (int i = 0; i < optExpression.arity(); ++i) {
                children.add(optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null));
            }
            return OptExpression.create(optExpression.getOp(), children);
        }
    }

    private class MVPartitionPrunerVisitor extends OptExpressionVisitor<OptExpression, Void> {
        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOperator = optExpression.getOp().cast();

            if (scanOperator instanceof LogicalOlapScanOperator) {
                LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) (scanOperator);
                builder.withOperator(olapScanOperator);
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
                LogicalOlapScanOperator newOlapScanOperator = builder.build();

                // prune partition
                List<Long> selectedPartitionIds = olapScanOperator.getSelectedPartitionId();
                if (selectedPartitionIds == null || selectedPartitionIds.isEmpty()) {
                    newOlapScanOperator =  OptOlapPartitionPruner.prunePartitions(newOlapScanOperator);
                }

                // prune distribution key
                newOlapScanOperator.buildColumnFilters(newOlapScanOperator.getPredicate());
                List<Long> selectedTabletIds = OptDistributionPruner.pruneTabletIds(newOlapScanOperator,
                        newOlapScanOperator.getSelectedPartitionId());

                ScalarOperator scanPredicate = newOlapScanOperator.getPredicate();
                if (isAddMvPrunePredicate) {
                    List<ScalarOperator> originConjuncts = Utils.extractConjuncts(scanOperator.getPredicate());
                    List<ScalarOperator> pruneConjuncts = Utils.extractConjuncts(mvRewriteContext.getMvPruneConjunct());
                    pruneConjuncts.removeAll(originConjuncts);
                    List<ScalarOperator> currentConjuncts = Utils.extractConjuncts(newOlapScanOperator.getPredicate());
                    currentConjuncts.removeAll(pruneConjuncts);
                    scanPredicate = Utils.compoundAnd(currentConjuncts);
                }

                LogicalOlapScanOperator.Builder rewrittenBuilder = new LogicalOlapScanOperator.Builder();
                scanOperator = rewrittenBuilder.withOperator(newOlapScanOperator)
                        .setPredicate(MvUtils.canonizePredicate(scanPredicate))
                        .setSelectedTabletId(selectedTabletIds)
                        .build();
            } else if (scanOperator instanceof LogicalHiveScanOperator ||
                    scanOperator instanceof LogicalHudiScanOperator ||
                    scanOperator instanceof LogicalIcebergScanOperator ||
                    scanOperator instanceof LogicalDeltaLakeScanOperator ||
                    scanOperator instanceof LogicalFileScanOperator ||
                    scanOperator instanceof LogicalEsScanOperator ||
                    scanOperator instanceof LogicalPaimonScanOperator) {
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

    /**
     * Rewrite specific olap scan operator with specific selected partition ids.
     */
    public static OptExpression getOlapTableCompensatePlan(OptExpression optExpression,
                                                           LogicalScanOperator mvRefScanOperator,
                                                           List<Long> refTableCompensatePartitionIds) {
        OptScanOperatorCompensator scanOperatorCompensator = new OptScanOperatorCompensator(
                refTableCompensatePartitionIds, mvRefScanOperator);
        return optExpression.getOp().accept(scanOperatorCompensator, optExpression, null);
    }

    /**
     * Rewrite specific external scan operator with specific selected partition ids.
     */
    public static OptExpression getExternalTableCompensatePlan(OptExpression optExpression,
                                                               LogicalScanOperator mvRefScanOperator,
                                                               ScalarOperator extraPredicate) {
        OptScanOperatorCompensator scanOperatorCompensator = new OptScanOperatorCompensator(
                mvRefScanOperator, extraPredicate);
        return optExpression.getOp().accept(scanOperatorCompensator, optExpression, null);
    }

    /**
     * Rewrite specific olap scan operator with specific selected partition ids.
     */
    static class OptScanOperatorCompensator extends OptExpressionVisitor<OptExpression, Void> {
        private final List<Long> olapTableCompensatePartitionIds;
        private final LogicalScanOperator refScanOperator;
        private final ScalarOperator externalExtraPredicate;

        // for olap table
        public OptScanOperatorCompensator(List<Long> refTableCompensatePartitionIds,
                                          LogicalScanOperator scanOperator) {
            this.refScanOperator = scanOperator;
            this.olapTableCompensatePartitionIds = refTableCompensatePartitionIds;
            this.externalExtraPredicate = null;
        }

        // for external table
        public OptScanOperatorCompensator(LogicalScanOperator mvRefScanOperator,
                                          ScalarOperator extraPredicate) {
            this.refScanOperator = mvRefScanOperator;
            this.olapTableCompensatePartitionIds = null;
            this.externalExtraPredicate = extraPredicate;
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOperator = optExpression.getOp().cast();
            if (scanOperator != refScanOperator) {
                return optExpression;
            }
            if (scanOperator instanceof LogicalOlapScanOperator) {
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;
                final LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
                Preconditions.checkState(olapTableCompensatePartitionIds != null);
                // reset original partition predicates to prune partitions/tablets again
                builder.withOperator(olapScanOperator)
                        .setSelectedPartitionId(olapTableCompensatePartitionIds)
                        .setSelectedTabletId(Lists.newArrayList());

                return OptExpression.create(builder.build());
            } else if (SUPPORTED_PARTITION_COMPENSATE_EXTERNAL_SCAN_TYPES.contains(scanOperator.getOpType())) {
                final LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(refScanOperator);
                // reset original partition predicates to prune partitions/tablets again
                builder.withOperator(refScanOperator);

                Preconditions.checkState(externalExtraPredicate != null);
                ScalarOperator finalPredicate = Utils.compoundAnd(refScanOperator.getPredicate(), externalExtraPredicate);
                builder.setPredicate(finalPredicate);
                if (scanOperator.getOpType() == OperatorType.LOGICAL_ICEBERG_SCAN) {
                    // refresh iceberg table's metadata
                    Table refBaseTable = refScanOperator.getTable();
                    IcebergTable cachedIcebergTable = (IcebergTable) refBaseTable;
                    String catalogName = cachedIcebergTable.getCatalogName();
                    String dbName = cachedIcebergTable.getRemoteDbName();
                    TableName tableName = new TableName(catalogName, dbName, cachedIcebergTable.getName());
                    Table currentTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(tableName).orElse(null);
                    if (currentTable == null) {
                        return null;
                    }
                    // Iceberg table's snapshot is cached in the mv's plan cache, need to reset it to get the latest snapshot
                    builder.setTable(currentTable);
                }
                LogicalScanOperator newScanOperator = builder.build();
                return OptExpression.create(newScanOperator);
            } else {
                return optExpression;
            }
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            List<OptExpression> children = Lists.newArrayList();
            for (int i = 0; i < optExpression.arity(); ++i) {
                children.add(optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null));
            }
            return OptExpression.create(optExpression.getOp(), children);
        }
    }
}
