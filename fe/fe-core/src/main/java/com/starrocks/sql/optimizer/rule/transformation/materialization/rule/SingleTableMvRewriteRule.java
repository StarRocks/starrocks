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


package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Binder;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

// used to rewrite query with single table scan pattern to mv
public class SingleTableMvRewriteRule extends Rule {
    public SingleTableMvRewriteRule() {
        super(RuleType.TF_MV_CBO_SINGLE_TABLE_REWRITE_RULE, Pattern.create(OperatorType.PATTERN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        rewrite(input, context);
        return Lists.newArrayList();
    }

    private void rewrite(OptExpression input, OptimizerContext context) {
        List<Rule> rules = context.getRuleSet().getRewriteRulesByType(RuleSetType.SINGLE_TABLE_MV_REWRITE);

        List<OptExpression> newExpressions = Lists.newArrayList();
        for (Rule rule : rules) {
            Pattern pattern = rule.getPattern();
            Binder binder = new Binder(pattern, input.getGroupExpression());
            OptExpression extractExpr = binder.next();

            while (extractExpr != null) {
                if (!rule.check(extractExpr, context)) {
                    extractExpr = binder.next();
                    continue;
                }
                List<OptExpression> targetExpressions = rule.transform(extractExpr, context);
                if (targetExpressions != null && !targetExpressions.isEmpty()) {
                    newExpressions.addAll(targetExpressions);
                }

                extractExpr = binder.next();
            }
        }
        OptimizerTraceUtil.logApplyRule(context.getSessionVariable(), context.getTraceInfo(), this, input, newExpressions);

        for (OptExpression expression : newExpressions) {
            // Insert new OptExpression to memo
            context.getMemo().copyIn(input.getGroupExpression().getGroup(), expression);
        }

        for (int i = input.getInputs().size() - 1; i >= 0; i--) {
            rewrite(input.getInputs().get(i), context);
        }
    }
}
