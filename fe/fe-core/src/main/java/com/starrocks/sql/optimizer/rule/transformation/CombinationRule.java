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

import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.OpPattern;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class CombinationRule extends TransformationRule {
    private final List<Rule> rules;
    private final Set<OperatorType> ops = Sets.newHashSet();

    public CombinationRule(RuleType ruleType, List<Rule> rules) {
        super(ruleType, Pattern.create(OperatorType.PATTERN_LEAF));
        this.rules = rules;

        if (rules.stream().allMatch(rule -> rule.getPattern().isFixedPattern())) {
            for (Rule rule : rules) {
                OperatorType type = ((OpPattern) rule.getPattern()).getOpType();
                ops.add(type);
            }
        }
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return ops.isEmpty() || ops.contains(input.getOp().getOpType());
    }

    @Override
    public List<Rule> predecessorRules() {
        return rules;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return Collections.emptyList();
    }
}
