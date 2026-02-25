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
package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OptConstFoldRewriter extends OptExpressionVisitor<OptExpression, Void> {
    private final ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
    public OptConstFoldRewriter() {
    }

    public static OptExpression rewrite(OptExpression root) {
        if (root == null) {
            return null;
        }
        return root.getOp().accept(new OptConstFoldRewriter(), root, null);
    }

    private ScalarOperator rewrite(ScalarOperator scalarOperator) {
        if (scalarOperator == null) {
            return null;
        }
        return rewriter.rewrite(scalarOperator, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);
    }

    private Projection rewrite(Projection projection) {
        if (projection == null) {
            return null;
        }
        Map<ColumnRefOperator, ScalarOperator> columnRefMap =
                projection.getColumnRefMap();
        if (columnRefMap == null) {
            return projection;
        }
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = columnRefMap.entrySet()
                .stream()
                .map(e -> Pair.create(e.getKey(), rewrite(e.getValue())))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
        return new Projection(newColumnRefMap);
    }

    private Operator rewriteOp(Operator operator) {
        return withCommon(operator).build();
    }

    private Operator.Builder withCommon(Operator operator) {
        final Operator.Builder builder = OperatorBuilderFactory.build(operator);
        builder.withOperator(operator);
        // projections
        builder.setProjection(rewrite(operator.getProjection()));
        // predicates
        builder.setPredicate(rewrite(operator.getPredicate()));
        return builder;
    }

    private List<OptExpression> visitChildren(OptExpression optExpression) {
        return optExpression.getInputs().stream().map(child -> {
            return child.getOp().accept(this, child, null);
        }).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public OptExpression visit(OptExpression optExpression, Void context) {
        return OptExpression.create(rewriteOp(optExpression.getOp()), visitChildren(optExpression));
    }

    @Override
    public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
        final LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
        final LogicalJoinOperator.Builder builder = (LogicalJoinOperator.Builder) withCommon(joinOperator);
        builder.setOnPredicate(rewrite(joinOperator.getOnPredicate()));
        return OptExpression.create(builder.build(), visitChildren(optExpression));
    }

    @Override
    public OptExpression visitLogicalAggregate(OptExpression optExpression, Void context) {
        final LogicalAggregationOperator operator = (LogicalAggregationOperator) optExpression.getOp();
        final LogicalAggregationOperator.Builder builder = (LogicalAggregationOperator.Builder) withCommon(operator);
        builder.setAggregations(operator.getAggregations()
                .entrySet().stream().map(e -> Pair.create(e.getKey(), (CallOperator) rewrite(e.getValue())))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
        return OptExpression.create(builder.build(), visitChildren(optExpression));
    }

    @Override
    public OptExpression visitLogicalWindow(OptExpression optExpression, Void context) {
        final LogicalWindowOperator operator = (LogicalWindowOperator) optExpression.getOp();
        final LogicalWindowOperator.Builder builder = (LogicalWindowOperator.Builder) withCommon(operator);
        builder.setPartitionExpressions(operator.getPartitionExpressions()
                .stream().map(this::rewrite).collect(Collectors.toList()));
        builder.setWindowCall(operator.getWindowCall().entrySet()
                .stream().map(e -> Pair.create(e.getKey(), (CallOperator) rewrite(e.getValue())))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
        return OptExpression.create(builder.build(), visitChildren(optExpression));
    }
}