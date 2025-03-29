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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_AGG_PRUNE_COLUMNS;

public class MVColumnPruner {
    private ColumnRefSet requiredOutputColumns;

    public OptExpression pruneColumns(OptExpression queryExpression, ColumnRefSet requiredOutputColumns) {
        this.requiredOutputColumns = requiredOutputColumns;
        if (queryExpression.getOp() instanceof LogicalFilterOperator) {
            OptExpression newQueryOptExpression = doPruneColumns(queryExpression.inputAt(0));
            Operator filterOp = queryExpression.getOp();
            Operator.Builder opBuilder = OperatorBuilderFactory.build(filterOp);
            opBuilder.withOperator(filterOp);
            return OptExpression.create(opBuilder.build(), newQueryOptExpression);
        } else {
            return doPruneColumns(queryExpression);
        }
    }

    public OptExpression doPruneColumns(OptExpression optExpression) {
        // TODO: remove this check after we support more operators.
        Projection projection = optExpression.getOp().getProjection();
        // OptExpression after mv rewrite must have projection.
        if (projection == null) {
            return optExpression;
        }
        // OptExpression after mv rewrite must have projection.
        return optExpression.getOp().accept(new ColumnPruneVisitor(), optExpression, null);
    }

    // Prune columns by top-down, only support SPJG/union operators.
    private class ColumnPruneVisitor extends OptExpressionVisitor<OptExpression, Void> {
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOperator = optExpression.getOp().cast();

            Projection projection = scanOperator.getProjection();
            if (projection != null) {
                projection.getColumnRefMap().values().forEach(s -> requiredOutputColumns.union(s.getUsedColumns()));
            }

            Set<ColumnRefOperator> outputColumns =
                    scanOperator.getColRefToColumnMetaMap().keySet().stream().filter(requiredOutputColumns::contains)
                            .collect(Collectors.toSet());
            outputColumns.addAll(Utils.extractColumnRef(scanOperator.getPredicate()));
            if (scanOperator instanceof LogicalOlapScanOperator) {
                ((LogicalOlapScanOperator) scanOperator).getPrunedPartitionPredicates()
                        .forEach(o -> outputColumns.addAll(Utils.extractColumnRef(o)));
            }
            if (outputColumns.isEmpty()) {
                outputColumns.add(
                        Utils.findSmallestColumnRefFromTable(scanOperator.getColRefToColumnMetaMap(), scanOperator.getTable()));
            }

            ImmutableMap.Builder<ColumnRefOperator, Column> columnRefColumnMapBuilder = new ImmutableMap.Builder<>();
            scanOperator.getColRefToColumnMetaMap().keySet().stream()
                    .filter(outputColumns::contains)
                    .forEach(key -> columnRefColumnMapBuilder.put(key, scanOperator.getColRefToColumnMetaMap().get(key)));
            ImmutableMap<ColumnRefOperator, Column> newColumnRefMap = columnRefColumnMapBuilder.build();
            if (newColumnRefMap.size() != scanOperator.getColRefToColumnMetaMap().size()) {
                Operator.Builder builder = OperatorBuilderFactory.build(scanOperator);
                builder.withOperator(scanOperator);
                LogicalScanOperator.Builder scanBuilder = (LogicalScanOperator.Builder) builder;
                scanBuilder.setColRefToColumnMetaMap(newColumnRefMap);
                ColumnRefSet outputRefSet = new ColumnRefSet(outputColumns);
                outputRefSet.except(requiredOutputColumns);
                if (!outputRefSet.isEmpty() && projection == null) {
                    // should add a projection
                    Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap();
                    for (ColumnRefOperator columnRefOperator : outputColumns) {
                        if (outputRefSet.contains(columnRefOperator)) {
                            continue;
                        }
                        projectionMap.put(columnRefOperator, columnRefOperator);
                    }
                    Projection newProjection = new Projection(projectionMap);
                    builder.setProjection(newProjection);
                }
                Operator newQueryOp = builder.build();
                return OptExpression.create(newQueryOp);
            } else {
                return optExpression;
            }
        }

        public OptExpression visitLogicalAggregate(OptExpression optExpression, Void context) {
            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            if (aggregationOperator.getPredicate() != null) {
                requiredOutputColumns.union(Utils.extractColumnRef(aggregationOperator.getPredicate()));
            }
            // It's safe to prune columns if the aggregation operator has been rewritten by mv since the rewritten
            // mv plan should be rollup from the original plan.
            // TODO: We can do this in more normal ways rather than only mv rewrite later,
            // issue: https://github.com/StarRocks/starrocks/issues/55285
            if (aggregationOperator.isOpRuleBitSet(OP_MV_AGG_PRUNE_COLUMNS)) {
                // project
                Projection newProjection = null;
                if (aggregationOperator.getProjection() != null) {
                    newProjection = new Projection(aggregationOperator.getProjection().getColumnRefMap());
                    newProjection.getColumnRefMap().values().forEach(s -> requiredOutputColumns.union(s.getUsedColumns()));
                }
                // group by
                final List<ColumnRefOperator> newGroupByKeys = aggregationOperator.getGroupingKeys()
                        .stream()
                        .filter(col -> requiredOutputColumns.contains(col))
                        .collect(Collectors.toList());
                requiredOutputColumns.union(newGroupByKeys);
                // partition by
                final List<ColumnRefOperator> newPartitionByKeys = aggregationOperator.getPartitionByColumns()
                        .stream()
                        .filter(col -> requiredOutputColumns.contains(col))
                        .collect(Collectors.toList());
                requiredOutputColumns.union(newPartitionByKeys);
                // aggregations
                final Map<ColumnRefOperator, CallOperator> newAggregations = aggregationOperator.getAggregations()
                        .entrySet()
                        .stream()
                        .filter(e -> requiredOutputColumns.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                newAggregations.values().stream().forEach(s -> requiredOutputColumns.union(s.getUsedColumns()));
                final LogicalAggregationOperator newAggOp = new LogicalAggregationOperator.Builder()
                        .withOperator(aggregationOperator)
                        .setProjection(newProjection)
                        .setGroupingKeys(newGroupByKeys)
                        .setPartitionByColumns(newPartitionByKeys)
                        .setAggregations(newAggregations)
                        .build();
                return OptExpression.create(newAggOp, visitChildren(optExpression));
            } else {
                if (aggregationOperator.getProjection() != null) {
                    Projection projection = aggregationOperator.getProjection();
                    projection.getColumnRefMap().values().forEach(s -> requiredOutputColumns.union(s.getUsedColumns()));
                }
                requiredOutputColumns.union(aggregationOperator.getGroupingKeys());
                for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
                    requiredOutputColumns.union(entry.getKey());
                    requiredOutputColumns.union(Utils.extractColumnRef(entry.getValue()));
                }
                List<OptExpression> children = visitChildren(optExpression);
                return OptExpression.create(aggregationOperator, children);
            }
        }

        public OptExpression visitLogicalUnion(OptExpression optExpression, Void context) {
            LogicalUnionOperator unionOperator = optExpression.getOp().cast();
            if (unionOperator.getProjection() != null) {
                Projection projection = optExpression.getOp().getProjection();
                projection.getColumnRefMap().values().forEach(s -> requiredOutputColumns.union(s.getUsedColumns()));
            }
            List<ColumnRefOperator> unionOutputColRefs = unionOperator.getOutputColumnRefOp();
            List<Integer> newUnionOutputIdxes = Lists.newArrayList();
            List<ColumnRefOperator> newUnionOutputColRefs = Lists.newArrayList();
            for (int i = 0; i < unionOutputColRefs.size(); i++) {
                ColumnRefOperator columnRefOperator = unionOutputColRefs.get(i);
                if (requiredOutputColumns.contains(columnRefOperator.getId())) {
                    newUnionOutputIdxes.add(i);
                    newUnionOutputColRefs.add(columnRefOperator);
                }
            }

            // if all output columns are selected, no need to prune
            if (newUnionOutputIdxes.size() == unionOutputColRefs.size()) {
                for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
                    requiredOutputColumns.union(optExpression.getChildOutputColumns(childIdx));
                }
                List<OptExpression> children = visitChildren(optExpression);
                return OptExpression.create(optExpression.getOp(), children);
            }
            // choose the smallest column ref if no column ref is selected to avoid empty output
            if (newUnionOutputIdxes.isEmpty()) {
                ColumnRefOperator smallestColumn = Utils.findSmallestColumnRef(unionOutputColRefs);
                newUnionOutputColRefs.add(smallestColumn);
                requiredOutputColumns.union(smallestColumn);
                newUnionOutputIdxes.add(unionOutputColRefs.indexOf(smallestColumn));
            }
            List<List<ColumnRefOperator>> newChildOutputColumns = Lists.newArrayList();
            for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
                final List<ColumnRefOperator> childOutputCols = unionOperator.getChildOutputColumns().get(childIdx);
                final List<ColumnRefOperator> newChildOutputCols = newUnionOutputIdxes.stream()
                        .map(idx -> childOutputCols.get(idx))
                        .collect(Collectors.toList());
                requiredOutputColumns.union(newChildOutputCols);
                newChildOutputColumns.add(newChildOutputCols);
            }
            LogicalUnionOperator newUnionOperator = new LogicalUnionOperator(newUnionOutputColRefs, newChildOutputColumns,
                    unionOperator.isUnionAll(), unionOperator.isFromIcebergEqualityDeleteRewrite());
            newUnionOperator.setProjection(optExpression.getOp().getProjection());
            List<OptExpression> children = visitChildren(optExpression);
            return OptExpression.create(newUnionOperator, children);
        }

        // NOTE: filter and join operator will not be kept in plan after mv rewrite,
        // keep these just for extendable.
        public OptExpression visitLogicalFilter(OptExpression optExpression, Void context) {
            LogicalFilterOperator filterOperator = (LogicalFilterOperator) optExpression.getOp();
            ColumnRefSet requiredInputColumns = filterOperator.getRequiredChildInputColumns();
            requiredOutputColumns.union(requiredInputColumns);
            List<OptExpression> children = visitChildren(optExpression);
            return OptExpression.create(filterOperator, children);
        }

        public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            requiredOutputColumns.union(joinOperator.getRequiredChildInputColumns());
            List<OptExpression> children = visitChildren(optExpression);
            return OptExpression.create(joinOperator, children);
        }

        public OptExpression visit(OptExpression optExpression, Void context) {
            throw UnsupportedException.unsupportedException(String.format("ColumnPruner does not support:%s", optExpression));
        }

        private List<OptExpression> visitChildren(OptExpression optExpression) {
            List<OptExpression> children = Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                children.add(child.getOp().accept(this, child, null));
            }
            return children;
        }
    }
}
