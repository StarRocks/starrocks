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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MVColumnPruner {
    private ColumnRefSet requiredOutputColumns;

    public OptExpression pruneColumns(OptExpression queryExpression) {
        Projection projection = queryExpression.getOp().getProjection();
        if (projection == null) {
            if (queryExpression.getOp() instanceof LogicalFilterOperator) {
                projection = queryExpression.inputAt(0).getOp().getProjection();
            }
            Preconditions.checkState(projection != null);
        }
        requiredOutputColumns = new ColumnRefSet(projection.getOutputColumns());
        return queryExpression.getOp().accept(new ColumnPruneVisitor(), queryExpression, null);
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
            if (outputColumns.size() == 0) {
                outputColumns.add(Utils.findSmallestColumnRef(
                        new ArrayList<>(scanOperator.getColRefToColumnMetaMap().keySet())));
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
                Operator newQueryOp = builder.build();
                return OptExpression.create(newQueryOp);
            } else {
                return optExpression;
            }
        }

        public OptExpression visitLogicalAggregate(OptExpression optExpression, Void context) {
            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
            if (aggregationOperator.getProjection() != null) {
                Projection projection = aggregationOperator.getProjection();
                projection.getColumnRefMap().values().forEach(s -> requiredOutputColumns.union(s.getUsedColumns()));
            }
            if (aggregationOperator.getPredicate() != null) {
                requiredOutputColumns.union(Utils.extractColumnRef(aggregationOperator.getPredicate()));
            }
            requiredOutputColumns.union(aggregationOperator.getGroupingKeys());
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationOperator.getAggregations().entrySet()) {
                requiredOutputColumns.union(entry.getKey());
                requiredOutputColumns.union(Utils.extractColumnRef(entry.getValue()));
            }
            List<OptExpression> children = visitChildren(optExpression);
            return OptExpression.create(aggregationOperator, children);
        }

        public OptExpression visitLogicalUnion(OptExpression optExpression, Void context) {
            for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
                requiredOutputColumns.union(optExpression.getChildOutputColumns(childIdx));
            }
            List<OptExpression> children = visitChildren(optExpression);
            return OptExpression.create(optExpression.getOp(), children);
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
