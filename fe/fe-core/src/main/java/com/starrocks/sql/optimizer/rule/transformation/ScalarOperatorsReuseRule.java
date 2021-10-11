// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorsReuse;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ScalarOperatorsReuseRule extends TransformationRule {
    public ScalarOperatorsReuseRule() {
        super(RuleType.TF_SCALAR_OPERATORS_REUSE,
                Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF));
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.getOp();
        return projectOperator.getCommonSubOperatorMap().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.getOp();

        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projectOperator.getColumnRefMap();
        List<ScalarOperator> scalarOperators = Lists.newArrayList(columnRefMap.values());

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>>
                commonSubOperatorsByDepth = ScalarOperatorsReuse.
                collectCommonSubScalarOperators(scalarOperators,
                        context.getColumnRefFactory());

        Map<ScalarOperator, ColumnRefOperator> commonSubOperators =
                commonSubOperatorsByDepth.values().stream()
                        .flatMap(m -> m.entrySet().stream())
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!commonSubOperators.isEmpty()) {
            /*
             * If no rewritted, we should return empty list
             */
            boolean hasRewritted = false;
            for (ScalarOperator operator : columnRefMap.values()) {
                ScalarOperator rewrittedOperator = ScalarOperatorsReuse.
                        rewriteOperatorWithCommonOperator(operator, commonSubOperators);
                if (!rewrittedOperator.equals(operator)) {
                    hasRewritted = true;
                    break;
                }
            }

            /*
             * 1. Rewrite the operator with the common sub operators
             * 2. Put the common sub operators to project node, we need to compute
             * common sub operators firstly in BE
             */
            if (hasRewritted) {
                Map<ColumnRefOperator, ScalarOperator> newMap = Maps.newHashMap();
                for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : columnRefMap.entrySet()) {
                    newMap.put(kv.getKey(), ScalarOperatorsReuse.
                            rewriteOperatorWithCommonOperator(kv.getValue(), commonSubOperators));
                }

                Map<ColumnRefOperator, ScalarOperator> newCommonMap = Maps.newHashMap();
                for (Map.Entry<ScalarOperator, ColumnRefOperator> kv : commonSubOperators.entrySet()) {
                    Preconditions.checkState(!newMap.containsKey(kv.getValue()));
                    newCommonMap.put(kv.getValue(), kv.getKey());
                }

                return Lists.newArrayList(OptExpression.create(
                        new LogicalProjectOperator(newMap, newCommonMap), input.getInputs()));
            }
        }

        return Collections.emptyList();
    }

}
