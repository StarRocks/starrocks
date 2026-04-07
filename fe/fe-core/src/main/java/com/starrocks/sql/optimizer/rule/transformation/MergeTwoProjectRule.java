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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MergeTwoProjectRule extends TransformationRule {
    public MergeTwoProjectRule() {
        super(RuleType.TF_MERGE_TWO_PROJECT, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator firstProject = (LogicalProjectOperator) input.getOp();
        LogicalProjectOperator secondProject = (LogicalProjectOperator) input.getInputs().get(0).getOp();

        // cteRewriteMaxFlatChildren > 0 means to enable CTE rewrite if the merged project has an expression whose flat children
        // count is above cteRewriteMaxFlatChildren.
        int cteRewriteMaxFlatChildren = Config.merge_project_cte_rewrite_max_flat_children;
        // We want to perform CTE rewrites only when the merged expression is more complicated than existing expressions. So we
        // calculate the max count of flat children from bottom project first.
        int maxBottomFlatChildren = 0;
        if (cteRewriteMaxFlatChildren > 0) {
            for (ScalarOperator expr : secondProject.getColumnRefMap().values()) {
                maxBottomFlatChildren = Math.max(maxBottomFlatChildren, expr.getNumFlatChildren());
            }
        }

        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(secondProject.getColumnRefMap());
        Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : firstProject.getColumnRefMap().entrySet()) {
            ScalarOperator result;
            result = rewriter.rewriteWithoutCheck(entry.getValue());
            // Create CTEs to avoid creating overly-complex expressions. Overly-complex expression may 1. fail the query
            // because of expression complexity limit; 2. result in long planning time; 3. consume more FE memory.
            // These conditions must be met:
            // - merge_project_cte_rewrite_max_flat_children is set to > 0
            // - result's flat children count exceeds merge_project_cte_rewrite_max_flat_children
            // - result's flat children count does increase after the merge
            // - result's flat children count is larger than max flat children count from bottom projects
            // These additional checks avoid creating unnecessary CTEs when an identity projection is chained with an already-
            // complex projection.
            int resultNumFlatChildren = result.getNumFlatChildren();
            if (cteRewriteMaxFlatChildren > 0
                    && resultNumFlatChildren > cteRewriteMaxFlatChildren
                    && resultNumFlatChildren > entry.getValue().getNumFlatChildren()
                    && resultNumFlatChildren > maxBottomFlatChildren) {
                return createCTEForComplexMerge(input, context, firstProject, secondProject);
            } else {
                result.checkMaxFlatChildren(true);
            }
            if (result.isConstant()) {
                // better to rewrite all expression, but it's unnecessary
                result = scalarRewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            }
            resultMap.put(entry.getKey(), result);
        }

        // ASSERT_TRUE must be executed in the runtime, so it should be kept anyway.
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : secondProject.getColumnRefMap().entrySet()) {
            if (entry.getValue() instanceof CallOperator) {
                CallOperator callOp = entry.getValue().cast();
                if (FunctionSet.ASSERT_TRUE.equals(callOp.getFnName())) {
                    resultMap.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // minimum value of limits on projections, but have to exclude unlimited(-1) case
        long limit = Stream.of(firstProject.getLimit(), secondProject.getLimit())
                .filter(l -> l >= 0)
                .min(Long::compare)
                .orElse(-1L);

        OptExpression optExpression = new OptExpression(
                new LogicalProjectOperator(resultMap, limit));
        optExpression.getInputs().addAll(input.getInputs().get(0).getInputs());
        return Lists.newArrayList(optExpression);
    }

    private List<OptExpression> createCTEForComplexMerge(OptExpression input, OptimizerContext context,
                                                          LogicalProjectOperator firstProject,
                                                          LogicalProjectOperator secondProject) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        int cteId = context.getCteContext().getNextCteId();

        OptExpression bottomTree = input.getInputs().get(0);
        OptExpression cteProduce = OptExpression.create(new LogicalCTEProduceOperator(cteId),
                Lists.newArrayList(bottomTree));

        // CTE consume: create new column refs that map to the bottom project's output columns
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> columnRefRewriteMap = Maps.newHashMap();

        for (ColumnRefOperator outputCol : secondProject.getColumnRefMap().keySet()) {
            ColumnRefOperator newCol = columnRefFactory.create(outputCol, outputCol.getType(), outputCol.isNullable());
            consumeOutputMap.put(newCol, outputCol);
            columnRefRewriteMap.put(outputCol, newCol);
        }

        LogicalCTEConsumeOperator cteConsume = new LogicalCTEConsumeOperator(cteId, consumeOutputMap);

        // Rewrite the top project to reference the CTE consume's output columns
        ReplaceColumnRefRewriter consumeRewriter = new ReplaceColumnRefRewriter(columnRefRewriteMap);
        Map<ColumnRefOperator, ScalarOperator> newFirstProjectMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : firstProject.getColumnRefMap().entrySet()) {
            newFirstProjectMap.put(entry.getKey(), consumeRewriter.rewrite(entry.getValue()));
        }

        LogicalProjectOperator newFirstProject = new LogicalProjectOperator(newFirstProjectMap, firstProject.getLimit());

        OptExpression consumeExpr = OptExpression.create(cteConsume);
        OptExpression topProjectExpr = OptExpression.create(newFirstProject, consumeExpr);

        LogicalCTEAnchorOperator cteAnchor = new LogicalCTEAnchorOperator(cteId);
        OptExpression result = OptExpression.create(cteAnchor, cteProduce, topProjectExpr);

        context.getCteContext().addForceCTE(cteId);

        return Lists.newArrayList(result);
    }
}
