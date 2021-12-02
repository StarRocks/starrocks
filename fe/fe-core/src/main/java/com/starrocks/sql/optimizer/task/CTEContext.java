// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;

import java.util.Map;

public class CTEContext {
    // All CTE produce
    private final Map<String, OptExpression> produces;

    // Nums of CTE consume
    private final Map<String, Integer> consumeNums;

    // Consume required columns
    private final Map<String, ColumnRefSet> requiredColumns;

    public CTEContext() {
        produces = Maps.newHashMap();
        consumeNums = Maps.newHashMap();
        requiredColumns = Maps.newHashMap();
    }

    public void init(OptExpression tree) {
        tree.getOp().accept(new CTEVisitor(), tree, this);
    }

    public OptExpression getCTEProduce(String cteId) {
        Preconditions.checkState(produces.containsKey(cteId));
        return produces.get(cteId);
    }

    public int getCTEConsumeNums(String cteId) {
        Preconditions.checkState(consumeNums.containsKey(cteId));
        return consumeNums.get(cteId);
    }

    public Map<String, ColumnRefSet> getRequiredColumns() {
        return requiredColumns;
    }

    public ColumnRefSet getAllRequiredColumns() {
        ColumnRefSet all = new ColumnRefSet();
        requiredColumns.values().forEach(all::union);
        return all;
    }

    private static class CTEVisitor extends OptExpressionVisitor<Void, CTEContext> {
        @Override
        public Void visit(OptExpression optExpression, CTEContext context) {
            for (OptExpression child : optExpression.getInputs()) {
                child.getOp().accept(this, child, context);
            }

            return null;
        }

        @Override
        public Void visitLogicalCTEProduce(OptExpression optExpression, CTEContext context) {
            LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) optExpression.getOp();
            context.produces.put(produce.getCteId(), optExpression);
            return visit(optExpression, context);
        }

        @Override
        public Void visitLogicalCTEConsume(OptExpression optExpression, CTEContext context) {
            LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) optExpression.getOp();

            int nums = context.consumeNums.getOrDefault(consume.getCteId(), 0);
            context.consumeNums.put(consume.getCteId(), nums + 1);

            ColumnRefSet requiredColumnRef =  context.requiredColumns.getOrDefault(consume.getCteId(), new ColumnRefSet());
            consume.getCteOutputColumnRefMap().values().forEach(requiredColumnRef::union);
            context.requiredColumns.put(consume.getCteId(), requiredColumnRef);

            return visit(optExpression, context);
        }
    }
}
