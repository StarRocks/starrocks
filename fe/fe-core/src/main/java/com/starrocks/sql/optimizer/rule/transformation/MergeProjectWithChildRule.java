// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class MergeProjectWithChildRule extends TransformationRule {
    public MergeProjectWithChildRule() {
        super(RuleType.TF_MERGE_PROJECT_WITH_CHILD,
                Pattern.create(OperatorType.LOGICAL_PROJECT).
                        addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator logicalProjectOperator = (LogicalProjectOperator) input.getOp();
        Operator child = input.inputAt(0).getOp();

        Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
        if (child.getProjection() != null) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(child.getProjection().getColumnRefMap());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : logicalProjectOperator.getColumnRefMap()
                    .entrySet()) {
                resultMap.put(entry.getKey(), entry.getValue().accept(rewriter, null));
            }
        } else {
            resultMap = logicalProjectOperator.getColumnRefMap();
        }

        Operator.Builder builder = OperatorBuilderFactory.build(child);
        builder.withOperator(child).setProjection(new Projection(resultMap));

        if (logicalProjectOperator.getLimit() != -1) {
            builder.setLimit(logicalProjectOperator.getLimit());
        } else {
            builder.setLimit(child.getLimit());
        }
        
        return Lists.newArrayList(OptExpression.create(builder.build(), input.inputAt(0).getInputs()));
    }
}
