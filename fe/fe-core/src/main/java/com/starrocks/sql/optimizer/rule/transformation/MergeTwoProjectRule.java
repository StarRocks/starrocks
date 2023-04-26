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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class MergeTwoProjectRule extends TransformationRule {
    public MergeTwoProjectRule() {
        super(RuleType.TF_MERGE_TWO_PROJECT, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator firstProject = (LogicalProjectOperator) input.getOp();
        LogicalProjectOperator secondProject = (LogicalProjectOperator) input.getInputs().get(0).getOp();

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(secondProject.getColumnRefMap());
        Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : firstProject.getColumnRefMap().entrySet()) {
            resultMap.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
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

        OptExpression optExpression = new OptExpression(
                new LogicalProjectOperator(resultMap, Math.min(firstProject.getLimit(), secondProject.getLimit())));
        optExpression.getInputs().addAll(input.getInputs().get(0).getInputs());
        return Lists.newArrayList(optExpression);
    }
}
