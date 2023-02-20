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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedPredicateRule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Optional;

public class PushDownPredicateProjectRule extends TransformationRule {
    private static final List<ScalarOperatorRewriteRule> PROJECT_REWRITE_PREDICATE_RULE = Lists.newArrayList(
            // Don't need add cast because all expression has been cast
            // The purpose here is for simplify the expression
            new ReduceCastRule(),
            new NormalizePredicateRule(),
            new FoldConstantsRule(),
            new SimplifiedPredicateRule()
    );

    private final ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();

    public PushDownPredicateProjectRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_PROJECT,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator secondProject = (LogicalProjectOperator) input.getInputs().get(0).getOp();
        Optional<ScalarOperator> assertColumn = secondProject.getColumnRefMap().values()
                .stream()
                .filter((op) -> {
                    if (!(op instanceof CallOperator)) {
                        return false;
                    }
                    return FunctionSet.ASSERT_TRUE.equals(((CallOperator) op).getFnName());
                })
                .findAny();
        return !assertColumn.isPresent();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filter = (LogicalFilterOperator) input.getOp();
        OptExpression child = input.getInputs().get(0);

        LogicalProjectOperator project = (LogicalProjectOperator) (child.getOp());
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(project.getColumnRefMap());
        ScalarOperator newPredicate = rewriter.rewrite(filter.getPredicate());

        // try rewrite new predicate
        // e.g. : select 1 as b, MIN(v1) from t0 having (b + 1) != b;
        newPredicate = scalarRewriter.rewrite(newPredicate, PROJECT_REWRITE_PREDICATE_RULE);

        OptExpression newFilter = new OptExpression(new LogicalFilterOperator(newPredicate));

        newFilter.getInputs().addAll(child.getInputs());
        child.getInputs().clear();
        child.getInputs().add(newFilter);

        return Lists.newArrayList(child);
    }

}
