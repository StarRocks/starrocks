// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.HashMap;
import java.util.Map;

// TODO: Need a better CPU cost estimation method
// estimate cpu costs for expression
public class ExpressionCpuCostsEstimate extends ScalarOperatorVisitor<Long, Void> {

    private static final Map<PrimitiveType, Integer> ESTIMATED_TYPE_COSTS = new HashMap<>();

    static {
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.TINYINT, 1);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.SMALLINT, 2);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.INT, 4);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.BIGINT, 8);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.FLOAT, 8);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.DOUBLE, 8);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.LARGEINT, 16);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.JSON, 32);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.VARCHAR, 32);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.CHAR, 32);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.HLL, 64);
        ESTIMATED_TYPE_COSTS.put(PrimitiveType.BITMAP, 64);
    }

    private long estimatedCpuCosts(Type type) {
        if (type.isComplexType()) {
            return 64;
        }
        return ESTIMATED_TYPE_COSTS.getOrDefault(type.getPrimitiveType(), 0);
    }

    @Override
    public Long visit(ScalarOperator scalarOperator, Void context) {
        long costs = 0;
        for (ScalarOperator child : scalarOperator.getChildren()) {
            costs += visit(child, context);
        }

        return costs + estimatedCpuCosts(scalarOperator.getType());
    }

    @Override
    public Long visitVariableReference(ColumnRefOperator node, Void context) {
        return (long) 0;
    }

    @Override
    public Long visitCall(CallOperator call, Void context) {
        long costs = 0;
        String fnName = call.getFnName();
        switch (fnName.toLowerCase()) {
            case "multiply":
            case "divide": {
                costs = estimatedCpuCosts(call.getType());
                costs *= costs;
                break;
            }
            default: {
                costs = estimatedCpuCosts(call.getType());
                break;
            }
        }

        for (ScalarOperator child : call.getChildren()) {
            costs += visit(child, context);
        }

        return costs;
    }

}
