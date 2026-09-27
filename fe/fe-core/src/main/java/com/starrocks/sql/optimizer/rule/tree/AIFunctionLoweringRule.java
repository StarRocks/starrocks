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

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.AiFunctionExtractor;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lowers AI scalar calls into explicit AI project boundaries before ordinary
 * optimizer rules can move or duplicate their containing expressions.
 */
public final class AIFunctionLoweringRule implements TreeRewriteRule {
    private boolean hasRewrite;

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        hasRewrite = false;
        OptExpression result = rewriteBottomUp(root, taskContext.getOptimizerContext());
        if (hasRewrite) {
            result.clearStatsAndInitOutputInfo();
            deriveLogicalProperties(result);
        }
        return result;
    }

    public boolean hasRewrite() {
        return hasRewrite;
    }

    private OptExpression rewriteBottomUp(OptExpression root, OptimizerContext context) {
        boolean childChanged = false;
        for (int childIndex = 0; childIndex < root.arity(); childIndex++) {
            OptExpression originalChild = root.inputAt(childIndex);
            OptExpression rewrittenChild = rewriteBottomUp(originalChild, context);
            if (rewrittenChild != originalChild) {
                root.setChild(childIndex, rewrittenChild);
                childChanged = true;
            }
        }
        if (childChanged || root.getLogicalProperty() == null) {
            root.deriveLogicalPropertyItself();
        }

        if (root.getOp() instanceof LogicalAIProjectOperator) {
            return root;
        }
        if (root.getOp() instanceof LogicalProjectOperator project
                && project.getColumnRefMap().values().stream().anyMatch(AiFunctionExtractor::containsAI)) {
            hasRewrite = true;
            return lowerProject(root, project, context);
        }
        if (root.getOp() instanceof LogicalFilterOperator filter
                && AiFunctionExtractor.containsAI(filter.getPredicate())) {
            hasRewrite = true;
            return lowerFilter(root, filter, context);
        }
        if (root.getOp() instanceof LogicalValuesOperator values
                && values.getRows().stream().flatMap(List::stream).anyMatch(AiFunctionExtractor::containsAI)) {
            hasRewrite = true;
            return rewriteBottomUp(lowerValues(values, context), context);
        }
        if (root.getOp() instanceof LogicalJoinOperator join
                && AiFunctionExtractor.containsAI(join.getOnPredicate())) {
            hasRewrite = true;
            return rewriteBottomUp(lowerJoin(root, join, context), context);
        }
        return root;
    }

    private OptExpression lowerProject(OptExpression input, LogicalProjectOperator project,
                                       OptimizerContext context) {
        List<Map.Entry<ColumnRefOperator, ScalarOperator>> entries =
                new ArrayList<>(project.getColumnRefMap().entrySet());
        AiFunctionExtractor.Result result = AiFunctionExtractor.extract(input.inputAt(0),
                entries.stream().map(Map.Entry::getValue).toList(), context);

        Map<ColumnRefOperator, ScalarOperator> rewrittenMap = new LinkedHashMap<>();
        for (int index = 0; index < entries.size(); index++) {
            rewrittenMap.put(entries.get(index).getKey(), result.expressions().get(index));
        }
        return derived(OptExpression.create(
                new LogicalProjectOperator(rewrittenMap, project.getLimit()), result.root()));
    }

    private OptExpression lowerFilter(OptExpression input, LogicalFilterOperator filter,
                                      OptimizerContext context) {
        ColumnRefSet originalOutputs = input.getOutputColumns();
        AiFunctionExtractor.Result result = AiFunctionExtractor.extract(
                input.inputAt(0), List.of(filter.getPredicate()), context);
        LogicalFilterOperator rewrittenFilter = new LogicalFilterOperator.Builder()
                .withOperator(filter)
                .setPredicate(result.expressions().get(0))
                .build();
        OptExpression root = derived(OptExpression.create(rewrittenFilter, result.root()));
        if (filter.getProjection() == null) {
            root = AiFunctionExtractor.retainOutputs(root, originalOutputs, context);
        }
        return root;
    }

    private OptExpression lowerValues(LogicalValuesOperator values, OptimizerContext context) {
        List<ColumnRefOperator> outputColumns = values.getColumnRefSet();
        List<OptExpression> branches = new ArrayList<>();
        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();

        for (int rowIndex = 0; rowIndex < values.getRows().size(); rowIndex++) {
            List<ColumnRefOperator> branchColumns = new ArrayList<>();
            for (ColumnRefOperator output : outputColumns) {
                branchColumns.add(values.getRows().size() == 1 ? output : context.getColumnRefFactory()
                        .create("values_ai_" + rowIndex, output.getType(), output.isNullable()));
            }
            List<ScalarOperator> placeholders = branchColumns.stream()
                    .map(column -> (ScalarOperator) ConstantOperator.createNull(column.getType())).toList();
            OptExpression branch = derived(OptExpression.create(
                    new LogicalValuesOperator(branchColumns, List.of(placeholders))));
            Map<ColumnRefOperator, ScalarOperator> rowProject = new LinkedHashMap<>();
            for (int columnIndex = 0; columnIndex < branchColumns.size(); columnIndex++) {
                rowProject.put(branchColumns.get(columnIndex), values.getRows().get(rowIndex).get(columnIndex));
            }
            branches.add(derived(OptExpression.create(new LogicalProjectOperator(rowProject), branch)));
            childOutputColumns.add(branchColumns);
        }

        OptExpression root;
        if (branches.size() == 1) {
            root = branches.get(0);
        } else {
            root = derived(OptExpression.create(
                    new LogicalUnionOperator(outputColumns, childOutputColumns, true), branches));
        }
        if (values.getPredicate() != null) {
            root = derived(OptExpression.create(new LogicalFilterOperator(values.getPredicate()), root));
        }
        root = AiFunctionExtractor.applyProjection(root, values.getProjection());
        if (values.hasLimit()) {
            root = derived(OptExpression.create(LogicalLimitOperator.init(values.getLimit()), root));
        }
        return root;
    }

    private OptExpression lowerJoin(OptExpression input, LogicalJoinOperator join,
                                    OptimizerContext context) {
        if (!join.isInnerOrCrossJoin()) {
            OptExpression largeInJoin = lowerLargeInJoin(input, join, context);
            if (largeInJoin != null) {
                return largeInJoin;
            }
            throw new SemanticException("AI functions in JOIN ON are supported only for INNER/CROSS joins");
        }

        ColumnRefSet originalOutputs = input.getOutputColumns();
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(join.getOnPredicate());
        List<ScalarOperator> aiConjuncts = conjuncts.stream()
                .filter(AiFunctionExtractor::containsAI).toList();
        List<ScalarOperator> localConjuncts = conjuncts.stream()
                .filter(predicate -> !AiFunctionExtractor.containsAI(predicate)).toList();
        ScalarOperator localOnPredicate = Utils.compoundAnd(localConjuncts);

        LogicalJoinOperator rewrittenJoin = LogicalJoinOperator.builder()
                .withOperator(join)
                .setJoinType(localOnPredicate == null ? JoinOperator.CROSS_JOIN : join.getJoinType())
                .setOnPredicate(localOnPredicate)
                .setOriginalOnPredicate(localOnPredicate)
                .setPredicate(null)
                .setProjection(null)
                .setLimit(Operator.DEFAULT_LIMIT)
                .build();
        OptExpression rawJoin = derived(OptExpression.create(rewrittenJoin, input.getInputs()));

        ScalarOperator movedPredicate = Utils.compoundAnd(aiConjuncts);
        if (join.getPredicate() != null) {
            movedPredicate = Utils.compoundAnd(movedPredicate, join.getPredicate());
        }
        AiFunctionExtractor.Result result = AiFunctionExtractor.extract(
                rawJoin, List.of(movedPredicate), context);
        OptExpression root = derived(OptExpression.create(
                new LogicalFilterOperator(result.expressions().get(0)), result.root()));
        if (join.getProjection() != null) {
            root = AiFunctionExtractor.applyProjection(root, join.getProjection());
        } else {
            root = AiFunctionExtractor.retainOutputs(root, originalOutputs, context);
        }
        if (join.hasLimit()) {
            root = derived(OptExpression.create(LogicalLimitOperator.init(join.getLimit()), root));
        }
        return root;
    }

    /**
     * LargeInPredicateToJoinRule runs immediately before mandatory AI lowering and turns
     * {@code expr IN (...)} into a left semi/anti join against LogicalRawValues. Lowering
     * the left-side expression before that join preserves the original per-input-row
     * evaluation semantics without enabling AI expressions in arbitrary semi/anti joins.
     */
    private OptExpression lowerLargeInJoin(OptExpression input, LogicalJoinOperator join,
                                           OptimizerContext context) {
        if ((join.getJoinType() != JoinOperator.LEFT_SEMI_JOIN &&
                join.getJoinType() != JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) ||
                input.arity() != 2 ||
                !(input.inputAt(1).getOp() instanceof LogicalRawValuesOperator rawValues) ||
                input.inputAt(1).arity() != 0 ||
                join.getPredicate() != null ||
                !(join.getOnPredicate() instanceof BinaryPredicateOperator binaryPredicate) ||
                binaryPredicate.getBinaryType() != BinaryType.EQ) {
            return null;
        }

        ScalarOperator leftOperand = binaryPredicate.getChild(0);
        ScalarOperator rightOperand = binaryPredicate.getChild(1);
        int aiOperandIndex;
        ScalarOperator aiOperand;
        ScalarOperator rawValuesOperand;
        if (AiFunctionExtractor.containsAI(leftOperand) && !AiFunctionExtractor.containsAI(rightOperand)) {
            aiOperandIndex = 0;
            aiOperand = leftOperand;
            rawValuesOperand = rightOperand;
        } else if (AiFunctionExtractor.containsAI(rightOperand) && !AiFunctionExtractor.containsAI(leftOperand)) {
            aiOperandIndex = 1;
            aiOperand = rightOperand;
            rawValuesOperand = leftOperand;
        } else {
            return null;
        }

        if (!(rawValuesOperand instanceof ColumnRefOperator rawValuesColumn) ||
                !rawValues.getColumnRefSet().contains(rawValuesColumn) ||
                !input.inputAt(0).getOutputColumns().containsAll(aiOperand.getUsedColumns())) {
            return null;
        }

        ColumnRefSet originalOutputs = input.getOutputColumns();
        AiFunctionExtractor.Result result =
                AiFunctionExtractor.extract(input.inputAt(0), List.of(aiOperand), context);
        ScalarOperator rewrittenOperand = result.expressions().get(0);
        BinaryPredicateOperator rewrittenPredicate = aiOperandIndex == 0
                ? new BinaryPredicateOperator(binaryPredicate.getBinaryType(), rewrittenOperand, rawValuesOperand)
                : new BinaryPredicateOperator(binaryPredicate.getBinaryType(), rawValuesOperand, rewrittenOperand);
        LogicalJoinOperator rewrittenJoin = LogicalJoinOperator.builder()
                .withOperator(join)
                .setOnPredicate(rewrittenPredicate)
                .setOriginalOnPredicate(rewrittenPredicate)
                .setProjection(null)
                .setLimit(Operator.DEFAULT_LIMIT)
                .build();
        OptExpression root = derived(OptExpression.create(
                rewrittenJoin, result.root(), input.inputAt(1)));
        if (join.getProjection() != null) {
            root = AiFunctionExtractor.applyProjection(root, join.getProjection());
        } else {
            root = AiFunctionExtractor.retainOutputs(root, originalOutputs, context);
        }
        if (join.hasLimit()) {
            root = derived(OptExpression.create(LogicalLimitOperator.init(join.getLimit()), root));
        }
        return root;
    }

    private static OptExpression derived(OptExpression expression) {
        expression.deriveLogicalPropertyItself();
        return expression;
    }

    private static void deriveLogicalProperties(OptExpression expression) {
        expression.getInputs().forEach(AIFunctionLoweringRule::deriveLogicalProperties);
        expression.deriveLogicalPropertyItself();
    }
}
