// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
case1:
         UNION                   UNION
      /    |     \       ->     /     \
   Empty  Child1  Child2     Child1  Child2

case2:
       UNION
      /      \     ->  Child1
   Child1    Empty
 */
public class PruneUnionEmptyRule extends TransformationRule {
    public PruneUnionEmptyRule() {
        super(RuleType.TF_PRUNE_UNION_EMPTY,
                Pattern.create(OperatorType.LOGICAL_UNION, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return input.getInputs().stream().map(OptExpression::getOp).filter(op -> op instanceof LogicalValuesOperator)
                .anyMatch(op -> ((LogicalValuesOperator) op).getRows().isEmpty());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalUnionOperator unionOperator = (LogicalUnionOperator) input.getOp();

        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        List<OptExpression> newInputs = Lists.newArrayList();

        for (int i = 0; i < input.getInputs().size(); i++) {
            // remove empty values
            OptExpression child = input.getInputs().get(i);
            if (!(child.getOp() instanceof LogicalValuesOperator &&
                    ((LogicalValuesOperator) child.getOp()).getRows().isEmpty())) {
                newInputs.add(child);
                childOutputColumns.add(unionOperator.getChildOutputColumns().get(i));
            }
        }

        if (newInputs.size() == 0) {
            return Lists.newArrayList(OptExpression
                    .create(new LogicalValuesOperator(unionOperator.getOutputColumnRefOp(), Collections.emptyList())));
        }

        if (newInputs.size() > 1) {
            return Lists.newArrayList(OptExpression
                    .create(new LogicalUnionOperator.Builder().withOperator((LogicalUnionOperator) input.getOp())
                            .setChildOutputColumns(childOutputColumns).build(), newInputs));
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();

        for (List<ColumnRefOperator> childOutputColumn : unionOperator.getChildOutputColumns()) {
            if (newInputs.get(0).getOutputColumns().isIntersect(new ColumnRefSet(childOutputColumn))) {
                for (int i = 0; i < unionOperator.getOutputColumnRefOp().size(); i++) {
                    ColumnRefOperator unionOutputColumn = unionOperator.getOutputColumnRefOp().get(i);
                    projectMap.put(unionOutputColumn, childOutputColumn.get(i));
                }
                break;
            }
        }

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);
        return Lists.newArrayList(OptExpression.create(projectOperator, newInputs));
    }
}
