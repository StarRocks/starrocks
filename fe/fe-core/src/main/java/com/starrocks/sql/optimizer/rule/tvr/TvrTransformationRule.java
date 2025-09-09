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

package com.starrocks.sql.optimizer.rule.tvr;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptMeta;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * TvrTransformationRule is an abstract class that provides common functionality for tvr transformation rules.
 */
public abstract class TvrTransformationRule extends TransformationRule {
    protected TvrTransformationRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    /**
     * OptExpressionWithOutput is a record that holds an OptExpression and its corresponding output column references.
     */
    public record OptExpressionWithOutput(OptExpression optExpression,
                                          List<ColumnRefOperator> outputColRefs) {
    }

    /**
     * Checks if the child expression is supported for tvr transformation, only if it has a TvrOptMeta.
     */
    protected boolean isChildSupportedTvr(OptExpression optExpression) {
        TvrOptMeta tvrOptMeta = optExpression.getTvrMeta();
        return tvrOptMeta != null;
    }

    /**
     * Whether the given OptExpression is supported for tvr transformation.
     */
    protected boolean isSupportedTvr(OptExpression optExpression) {
        // from bottom to top, check if the expression is supported for tvr transformation
        if (optExpression.getInputs().stream()
                .anyMatch(input -> !isChildSupportedTvr(input))) {
            return false;
        }
        // only if the current expression has no tvr group, it is considered for transformation
        return optExpression.getTvrMeta() == null;
    }

    protected OptExpressionDuplicator getOptDuplicator(OptimizerContext optimizerContext) {
        return new OptExpressionDuplicator(optimizerContext.getColumnRefFactory(), optimizerContext);
    }

    protected OptExpressionWithOutput duplicateOptExpression(OptimizerContext optimizerContext,
                                                             OptExpression optExpression,
                                                             List<ColumnRefOperator> originalOutputColRefs) {
        // TODO: what if the child has been duplicated, how to duplicate incrementally?
        // TODO: how to ensure the output column references are correctly mapped?
        final OptExpressionDuplicator duplicator = getOptDuplicator(optimizerContext);
        final OptExpression duplicated = duplicator.duplicate(optExpression);
        final List<ColumnRefOperator> outputColRefs = duplicator.getMappedColumns(originalOutputColRefs);
        Preconditions.checkArgument(outputColRefs.size() == originalOutputColRefs.size(),
                "Output column references size mismatch: expected %s, got %s",
                originalOutputColRefs.size(), outputColRefs.size());
        Preconditions.checkArgument(outputColRefs.stream().allMatch(Objects::nonNull),
                "Output column references should not contain nulls: %s", outputColRefs);
        return new OptExpressionWithOutput(duplicated, outputColRefs);
    }

    protected LogicalJoinOperator buildNewJoinOperator(LogicalJoinOperator join) {
        // Create a new LogicalJoinOperator with the same properties as the original one
        return new LogicalJoinOperator.Builder()
                .withOperator(join)
                .build();
    }

    protected OptExpressionWithOutput buildJoinOptExpression(OptimizerContext optimizerContext,
                                                             List<ColumnRefOperator> originalOutputColRefs,
                                                             LogicalJoinOperator newJoin,
                                                             OptExpression leftChild,
                                                             OptExpression rightChild,
                                                             boolean isDuplicateOptExpression) {
        OptExpression result = OptExpression.create(newJoin, leftChild, rightChild);
        if (!isDuplicateOptExpression) {
            // If we do not need to duplicate the expression, we can directly return the result
            return new OptExpressionWithOutput(result, originalOutputColRefs);
        } else {
            return duplicateOptExpression(optimizerContext, result, originalOutputColRefs);
        }
    }

    protected OptExpression buildUnionOperator(TvrOptMeta tvrOptMeta,
                                               List<ColumnRefOperator> outputColRefs,
                                               List<OptExpressionWithOutput> children) {
        List<List<ColumnRefOperator>> childOutColRefs = children.stream()
                .map(OptExpressionWithOutput::outputColRefs)
                .collect(Collectors.toList());
        List<OptExpression> childrenExpressions = children.stream()
                .map(OptExpressionWithOutput::optExpression)
                .collect(Collectors.toList());
        LogicalUnionOperator logicalUnionOperator =
                new LogicalUnionOperator(outputColRefs, childOutColRefs, true);
        return OptExpression.create(logicalUnionOperator, tvrOptMeta, childrenExpressions);
    }
}
