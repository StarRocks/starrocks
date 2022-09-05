// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MergeLimitWithSortRuleTest {

    @Test
    public void transform() {
        OptExpression limit = new OptExpression(LogicalLimitOperator.init(10, 2));
        OptExpression sort = new OptExpression(new LogicalTopNOperator(
                Lists.newArrayList(new Ordering(new ColumnRefOperator(1, Type.INT, "name", true), false, false))));

        limit.getInputs().add(sort);

        MergeLimitWithSortRule rule = new MergeLimitWithSortRule();
        List<OptExpression> list = rule.transform(limit, new OptimizerContext(new Memo(), new ColumnRefFactory()));

        assertEquals(OperatorType.LOGICAL_TOPN, list.get(0).getOp().getOpType());
        assertEquals(2, ((LogicalTopNOperator) list.get(0).getOp()).getOffset());
        assertEquals(10, ((LogicalTopNOperator) list.get(0).getOp()).getLimit());
    }
}