// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.CTEContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CollectCTEConsumeRule extends TransformationRule {
    public CollectCTEConsumeRule() {
        super(RuleType.TF_COLLECT_CTE_CONSUME, Pattern.create(OperatorType.LOGICAL_CTE_CONSUME));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) input.getOp();
        CTEContext cteContext = context.getCteContext();

        // record cte consume
        cteContext.addCTEConsume(consume.getCteId());

        // collect limit
        if (consume.hasLimit()) {
            if (cteContext.getConsumeLimits().containsKey(consume.getCteId())) {
                cteContext.getConsumeLimits().get(consume.getCteId()).add(consume.getLimit());
            } else {
                cteContext.getConsumeLimits().put(consume.getCteId(), Lists.newArrayList(consume.getLimit()));
            }
        }

        // collect predicate, should rewrite by CTE produce column
        if (null != consume.getPredicate()) {
            Map<ColumnRefOperator, ScalarOperator> mapping = Maps.newHashMap(consume.getCteOutputColumnRefMap());
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(mapping);
            ScalarOperator predicate = rewriter.rewrite(consume.getPredicate());

            if (cteContext.getConsumePredicates().containsKey(consume.getCteId())) {
                cteContext.getConsumePredicates().get(consume.getCteId()).add(predicate);
            } else {
                cteContext.getConsumePredicates().put(consume.getCteId(), Lists.newArrayList(predicate));
            }
        }

        return Collections.emptyList();
    }
}
