// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

public class JoinCommutativityWithOutInnerRule extends TransformationRule {
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

    private JoinCommutativityWithOutInnerRule() {
        super(RuleType.TF_JOIN_COMMUTATIVITY_WITHOUT_INNER, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF),
                        Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static final JoinCommutativityWithOutInnerRule INSTANCE = new JoinCommutativityWithOutInnerRule();

    public static JoinCommutativityWithOutInnerRule getInstance() {
        return INSTANCE;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        return ((LogicalJoinOperator) input.getOp()).getJoinHint().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return JoinCommutativityRule.commuteJoin(input, JOIN_COMMUTATIVITY_MAP);
    }
}
