// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JoinCommutativityRule extends TransformationRule {
    private static final Map<JoinOperator, JoinOperator> JOIN_COMMUTATIVITY_MAP =
            ImmutableMap.<JoinOperator, JoinOperator>builder()
                    .put(JoinOperator.LEFT_ANTI_JOIN, JoinOperator.RIGHT_ANTI_JOIN)
                    .put(JoinOperator.RIGHT_ANTI_JOIN, JoinOperator.LEFT_ANTI_JOIN)
                    .put(JoinOperator.LEFT_SEMI_JOIN, JoinOperator.RIGHT_SEMI_JOIN)
                    .put(JoinOperator.RIGHT_SEMI_JOIN, JoinOperator.LEFT_SEMI_JOIN)
                    .put(JoinOperator.LEFT_OUTER_JOIN, JoinOperator.RIGHT_OUTER_JOIN)
                    .put(JoinOperator.RIGHT_OUTER_JOIN, JoinOperator.LEFT_OUTER_JOIN)
                    .put(JoinOperator.INNER_JOIN, JoinOperator.INNER_JOIN)
                    .put(JoinOperator.CROSS_JOIN, JoinOperator.CROSS_JOIN)
                    .put(JoinOperator.FULL_OUTER_JOIN, JoinOperator.FULL_OUTER_JOIN)
                    .build();

    private JoinCommutativityRule() {
        super(RuleType.TF_JOIN_COMMUTATIVITY, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF),
                        Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static final JoinCommutativityRule INSTANCE = new JoinCommutativityRule();

    public static JoinCommutativityRule getInstance() {
        return INSTANCE;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        return ((LogicalJoinOperator) input.getOp()).getJoinHint().isEmpty();
    }

    public static List<OptExpression> commuteJoin(OptExpression input,
                                                  Map<JoinOperator, JoinOperator> commuteMap) {
        LogicalJoinOperator oldJoin = (LogicalJoinOperator) input.getOp();
        if (!commuteMap.containsKey(oldJoin.getJoinType())) {
            return Collections.emptyList();
        }

        List<OptExpression> newChildren = Lists.newArrayList(input.inputAt(1), input.inputAt(0));

        LogicalJoinOperator newJoin = new LogicalJoinOperator.Builder().withOperator(oldJoin)
                .setJoinType(commuteMap.get(oldJoin.getJoinType())).build();
        OptExpression result = OptExpression.create(newJoin, newChildren);
        return Lists.newArrayList(result);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return commuteJoin(input, JOIN_COMMUTATIVITY_MAP);
    }
}
