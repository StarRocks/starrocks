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

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class JoinCommutativityWithoutInnerRule extends TransformationRule {

    // NOTE: not support transform LEFT ANTI/SEMI to RIGHT ANTI/SEMI, since we don't want to support it
    private static final Map<JoinOperator, JoinOperator> JOIN_COMMUTATIVITY_MAP =
            ImmutableMap.<JoinOperator, JoinOperator>builder()
                    .put(JoinOperator.LEFT_ANTI_JOIN, JoinOperator.RIGHT_ANTI_JOIN)
                    .put(JoinOperator.RIGHT_ANTI_JOIN, JoinOperator.LEFT_ANTI_JOIN)
                    .put(JoinOperator.LEFT_SEMI_JOIN, JoinOperator.RIGHT_SEMI_JOIN)
                    .put(JoinOperator.RIGHT_SEMI_JOIN, JoinOperator.LEFT_SEMI_JOIN)
                    .put(JoinOperator.LEFT_OUTER_JOIN, JoinOperator.RIGHT_OUTER_JOIN)
                    .put(JoinOperator.RIGHT_OUTER_JOIN, JoinOperator.LEFT_OUTER_JOIN)
                    .put(JoinOperator.FULL_OUTER_JOIN, JoinOperator.FULL_OUTER_JOIN)
                    .build();

    private JoinCommutativityWithoutInnerRule() {
        super(RuleType.TF_JOIN_COMMUTATIVITY_WITHOUT_INNER, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF),
                        Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static final JoinCommutativityWithoutInnerRule INSTANCE = new JoinCommutativityWithoutInnerRule();

    public static JoinCommutativityWithoutInnerRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        return ((LogicalJoinOperator) input.getOp()).getJoinHint().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return JoinCommutativityRule.commuteJoin(input, JOIN_COMMUTATIVITY_MAP);
    }
}
