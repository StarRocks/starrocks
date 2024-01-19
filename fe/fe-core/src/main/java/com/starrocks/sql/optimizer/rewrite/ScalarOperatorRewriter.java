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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.ArithmeticCommutativeRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ExtractCommonPredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.MvNormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedPredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedScanColumnRule;

import java.util.List;
import java.util.stream.Collectors;

public class ScalarOperatorRewriter {
    public static final List<ScalarOperatorRewriteRule> DEFAULT_TYPE_CAST_RULE = Lists.newArrayList(
            new ImplicitCastRule()
    );

    public static final List<ScalarOperatorRewriteRule> DEFAULT_REWRITE_RULES = Lists.newArrayList(
            // required
            new ImplicitCastRule(),
            // optional
            new ReduceCastRule(),
            new NormalizePredicateRule(),
            new FoldConstantsRule(),
            new SimplifiedPredicateRule(),
            new ExtractCommonPredicateRule(),
            new ArithmeticCommutativeRule()
    );

    public static final List<ScalarOperatorRewriteRule> DEFAULT_REWRITE_SCAN_PREDICATE_RULES = Lists.newArrayList(
            // required
            new ImplicitCastRule(),
            // optional
            new ReduceCastRule(),
            new NormalizePredicateRule(),
            new FoldConstantsRule(),
            new SimplifiedScanColumnRule(),
            new SimplifiedPredicateRule(),
            new ExtractCommonPredicateRule(),
            new ArithmeticCommutativeRule()
    );
<<<<<<< HEAD

    public static final List<ScalarOperatorRewriteRule> MV_SCALAR_REWRITE_RULES = Lists.newArrayList(
            // required
            new ImplicitCastRule(),
            // optional
            new ReduceCastRule(),
            new MvNormalizePredicateRule(),
            new FoldConstantsRule(),
            new SimplifiedPredicateRule(),
            new ExtractCommonPredicateRule(),
            new ArithmeticCommutativeRule()
    );
=======
    public static final List<ScalarOperatorRewriteRule> MV_SCALAR_REWRITE_RULES = DEFAULT_REWRITE_SCAN_PREDICATE_RULES.stream()
            .map(rule -> rule instanceof NormalizePredicateRule ? new MvNormalizePredicateRule() : rule)
            .collect(Collectors.toList());
>>>>>>> d4f029d32b ([BugFix] Fix mv rewrite unknown error for query with IsNullPredicate (#39075))

    private final ScalarOperatorRewriteContext context;

    public ScalarOperatorRewriter() {
        context = new ScalarOperatorRewriteContext();
    }

    public ScalarOperator rewrite(ScalarOperator root, List<ScalarOperatorRewriteRule> ruleList) {
        ScalarOperator result = root;

        context.reset();
        int changeNums;
        do {
            changeNums = context.changeNum();
            for (ScalarOperatorRewriteRule rule : ruleList) {
                result = rewriteByRule(result, rule);
            }

            if (changeNums > Config.max_planner_scalar_rewrite_num) {
                throw new StarRocksPlannerException("Planner rewrite scalar operator over limit",
                        ErrorType.INTERNAL_ERROR);
            }
        } while (changeNums != context.changeNum());

        return result;
    }

    private ScalarOperator rewriteByRule(ScalarOperator root, ScalarOperatorRewriteRule rule) {
        ScalarOperator result = root;
        int changeNums;
        if (rule.isBottomUp()) {
            do {
                changeNums = context.changeNum();
                result = applyRuleBottomUp(result, rule);
            } while (changeNums != context.changeNum());
        } else if (rule.isTopDown()) {
            do {
                changeNums = context.changeNum();
                result = applyRuleTopDown(result, rule);
            } while (changeNums != context.changeNum());
        } else if (rule.isOnlyOnce()) {
            result = applyRuleOnlyOnce(result, rule);
        }

        return result;
    }

    private ScalarOperator applyRuleBottomUp(ScalarOperator operator, ScalarOperatorRewriteRule rule) {
        for (int i = 0; i < operator.getChildren().size(); i++) {
            operator.setChild(i, applyRuleBottomUp(operator.getChild(i), rule));
        }

        ScalarOperator op = rule.apply(operator, context);
        if (op != operator) {
            context.change();
        }
        return op;
    }

    private ScalarOperator applyRuleOnlyOnce(ScalarOperator operator, ScalarOperatorRewriteRule rule) {
        return rule.apply(operator, context);
    }

    private ScalarOperator applyRuleTopDown(ScalarOperator operator, ScalarOperatorRewriteRule rule) {
        ScalarOperator op = rule.apply(operator, context);
        if (op != operator) {
            context.change();
        }

        for (int i = 0; i < op.getChildren().size(); i++) {
            op.setChild(i, applyRuleTopDown(op.getChild(i), rule));
        }
        return op;
    }
}
