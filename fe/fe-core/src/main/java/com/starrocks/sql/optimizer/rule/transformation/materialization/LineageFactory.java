// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

// get all projection map under root
public class LineageFactory {
    private OptExpression root;
    private ColumnRefFactory refFactory;
    private Map<ColumnRefOperator, ScalarOperator> lineage;

    public LineageFactory(OptExpression root, ColumnRefFactory refFactory) {
        this.root = root;
        this.refFactory = refFactory;
        this.lineage = Maps.newHashMap();
    }

    public Map<ColumnRefOperator, ScalarOperator> getLineage() {
        LineageVisitor visitor = new LineageVisitor();
        if (root != null) {
            root.getOp().accept(visitor, root, null);
        }
        return lineage;
    }

    private class LineageVisitor extends OptExpressionVisitor<Void, Void> {
        @Override
        public Void visit(OptExpression optExpression, Void context) {
            if (!(optExpression.getOp() instanceof LogicalOperator)) {
                return null;
            }
            Map<ColumnRefOperator, ScalarOperator> projection = Utils.getColumnRefMap(optExpression, refFactory);
            lineage.putAll(projection);
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }
    }
}
