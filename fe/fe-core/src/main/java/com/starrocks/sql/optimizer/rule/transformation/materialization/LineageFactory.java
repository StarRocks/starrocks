package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LineageFactory {
    private OptExpression root;
    private ColumnRefFactory refFactory;
    private Map<ColumnRefOperator, ScalarOperator> lineage;

    public LineageFactory(OptExpression root, ColumnRefFactory refFactory) {
        this.root = root;
        this.lineage = Maps.newHashMap();
    }

    public Map<ColumnRefOperator, ScalarOperator> getLineage() {
        LineageVisitor visitor = new LineageVisitor();
        if (root != null) {
            root.getOp().accept(visitor, root,null);
        }
        return lineage;
    }

    private class LineageVisitor extends OptExpressionVisitor<Void, Void> {
        public Void visit(OptExpression optExpression, Void context) {
            if (!(optExpression.getOp() instanceof LogicalOperator)) {
                return null;
            }
            Map<ColumnRefOperator, ScalarOperator> projection = getProjectionMap(optExpression);
            // lineage should not contain the mapping
            Preconditions.checkState(!projection.keySet().stream().anyMatch(key -> lineage.containsKey(key)));
            lineage.putAll(projection);
            return optExpression.getOp().accept(this, optExpression, context);
        }
    }

    private Map<ColumnRefOperator, ScalarOperator> getProjectionMap(OptExpression expression) {
        if (expression.getOp().getProjection() != null) {
            return expression.getOp().getProjection().getColumnRefMap();
        } else {
            ColumnRefSet refSet = expression.getOutputColumns();
            Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap();
            for (int columnId : refSet.getColumnIds()) {
                ColumnRefOperator columnRef = refFactory.getColumnRef(columnId);
                projectionMap.put(columnRef, columnRef);
            }
            return projectionMap;
        }
    }
}
