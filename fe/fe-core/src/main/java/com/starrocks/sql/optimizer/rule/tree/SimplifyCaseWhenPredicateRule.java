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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimplifyCaseWhenPredicateRule implements TreeRewriteRule {
    public static final SimplifyCaseWhenPredicateRule INSTANCE = new SimplifyCaseWhenPredicateRule();

    private SimplifyCaseWhenPredicateRule() {
    }

    private static final class Rewriter extends OptExpressionVisitor<Optional<OptExpression>, Void> {
        public static final Rewriter INSTANCE = new Rewriter();

        private Rewriter() {
        }

        @Override
        public Optional<OptExpression> visit(OptExpression optExpression, Void context) {
            ScalarOperator predicate = optExpression.getOp().getPredicate();
            if (predicate == null) {
                return Optional.empty();
            }
            ScalarOperator newPredicate = ScalarOperatorRewriter.simplifyCaseWhen(predicate);
            if (newPredicate == predicate) {
                return Optional.empty();
            }
            Operator newOperator = OperatorBuilderFactory.build(optExpression.getOp())
                    .withOperator(optExpression.getOp())
                    .setPredicate(newPredicate).build();
            return Optional.of(OptExpression.create(newOperator, optExpression.getInputs()));
        }

        @Override
        public Optional<OptExpression> visitLogicalJoin(OptExpression optExpression, Void context) {
            LogicalJoinOperator joinOperator = optExpression.getOp().cast();
            Optional<ScalarOperator> optNewOnPredicate =
                    Optional.ofNullable(joinOperator.getOnPredicate()).map(predicate -> {
                        ScalarOperator newPredicate = ScalarOperatorRewriter.simplifyCaseWhen(predicate);
                        return newPredicate == predicate ? null : newPredicate;
                    });
            Optional<ScalarOperator> optNewPredicate =
                    Optional.ofNullable(joinOperator.getPredicate()).map(predicate -> {
                        ScalarOperator newPredicate = ScalarOperatorRewriter.simplifyCaseWhen(predicate);
                        return newPredicate == predicate ? null : newPredicate;
                    });
            if (!optNewOnPredicate.isPresent() && !optNewPredicate.isPresent()) {
                return Optional.empty();
            }
            Operator newOperator = LogicalJoinOperator.builder().withOperator(joinOperator)
                    .setOnPredicate(optNewOnPredicate.orElse(joinOperator.getOnPredicate()))
                    .setPredicate(optNewPredicate.orElse(joinOperator.getPredicate()))
                    .build();
            return Optional.of(OptExpression.create(newOperator, optExpression.getInputs()));
        }

        private Optional<OptExpression> processImpl(OptExpression optExpression) {
            List<Optional<OptExpression>> optNewInputs =
                    optExpression.getInputs().stream()
                            .map(this::processImpl)
                            .collect(Collectors.toList());
            List<OptExpression> newInputs = IntStream.range(0, optNewInputs.size())
                    .mapToObj(i -> optNewInputs.get(i).orElse(optExpression.getInputs().get(i)))
                    .collect(Collectors.toList());

            OptExpression newExpression = OptExpression.create(optExpression.getOp(), newInputs);
            return optExpression.getOp().accept(this, newExpression, null);
        }

        public OptExpression process(OptExpression optExpression) {
            return processImpl(optExpression).orElse(optExpression);
        }
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (taskContext.getOptimizerContext().getSessionVariable().isEnableSimplifyCaseWhen()) {
            return Rewriter.INSTANCE.process(root);
        } else {
            return root;
        }
    }
}
