// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;


import java.util.List;
import java.util.Map;

public class CorrelatedPredicateRewriter extends BaseScalarOperatorShuttle {

    private final ColumnRefSet correlationColSet;

    private final Map<ScalarOperator, ColumnRefOperator> exprToColumnRefMap;

    private final OptimizerContext optimizerContext;

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefToExprMap() {
        Map<ColumnRefOperator, ScalarOperator> columnRefToExprMap = Maps.newHashMap();
        exprToColumnRefMap.entrySet().stream().forEach(e -> columnRefToExprMap.put(e.getValue(), e.getKey()));
        return columnRefToExprMap;
    }

    public CorrelatedPredicateRewriter(List<ColumnRefOperator> correlationCols, OptimizerContext optimizerContext) {
        correlationColSet = new ColumnRefSet(correlationCols);
        this.optimizerContext = optimizerContext;
        exprToColumnRefMap = Maps.newHashMap();
    }

    @Override
    public ScalarOperator visit(ScalarOperator operator, Void context) {
        return operator;
    }

    @Override
    public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
        ColumnRefSet usedColumns = variable.getUsedColumns();

        if (correlationColSet.containsAll(usedColumns)) {
            return variable;
        }
        exprToColumnRefMap.putIfAbsent(variable, variable);
        return variable;
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, Void context) {
        ColumnRefSet usedColumns = call.getUsedColumns();

        if (correlationColSet.containsAll(usedColumns)) {
            return call;
        }

        if (!correlationColSet.isIntersect(usedColumns)) {
            return addExprToColumnRefMap(call);
        }

        return super.visitCall(call, context);
    }

    @Override
    public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
        ColumnRefSet usedColumns = operator.getUsedColumns();

        if (correlationColSet.containsAll(usedColumns)) {
            return operator;
        }

        if (!correlationColSet.isIntersect(usedColumns)) {
            return addExprToColumnRefMap(operator);
        }

        return super.visitCaseWhenOperator(operator, context);
    }

    @Override
    public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
        ColumnRefSet usedColumns = operator.getUsedColumns();

        if (correlationColSet.containsAll(usedColumns)) {
            return operator;
        }

        if (!correlationColSet.isIntersect(usedColumns)) {
            return addExprToColumnRefMap(operator);
        }
        return super.visitCastOperator(operator, context);
    }

    private ScalarOperator addExprToColumnRefMap(ScalarOperator operator) {
        if (!exprToColumnRefMap.containsKey(operator)) {
            ColumnRefOperator columnRefOperator = createColumnRefOperator(operator);
            exprToColumnRefMap.put(operator, columnRefOperator);
            return columnRefOperator;
        } else {
            return operator;
        }
    }
    private ColumnRefOperator createColumnRefOperator(ScalarOperator operator) {
        return optimizerContext.getColumnRefFactory().create(operator, operator.getType(), operator.isNullable());
    }
}
