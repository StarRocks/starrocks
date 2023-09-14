// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;
import java.util.Map;

/**
 * used to extract distinct expr which only contains inner table column and replace it with a new columnRef.
 * e.g. t1 is inner table, t2 is outer table. correlated predicate is:
 * t1.col1 + t2.col1 = abs(t1.col2) + t2.col1 + concat(t1.col3, t2.col1) + abs(t1.col2)
 * The rewriter will extract abs(t1.col2), t1.col1, t1.col3 and build scalarOperator to new columnRef map like:
 * t1.col1 : t1.col1
 * t1.col3 : t1.col3
 * abs(t1.col2) : columnRef1
 * <p>
 * then and rewrite the predicate like:
 * t1.col1 + t2.col1 = columnRef1 + t2.col1 + concat(t1.col3, t2.col1) + columnRef1
 */
public class CorrelatedPredicateRewriter extends ScalarOperatorVisitor<ScalarOperator, Void> {

    private final ColumnRefSet correlationColSet;

    private final Map<ScalarOperator, ColumnRefOperator> exprToColumnRefMap;

    private final OptimizerContext optimizerContext;

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefToExprMap() {
        Map<ColumnRefOperator, ScalarOperator> columnRefToExprMap = Maps.newHashMap();
        exprToColumnRefMap.forEach((key, value) -> columnRefToExprMap.put(value, key));
        return columnRefToExprMap;
    }

    public CorrelatedPredicateRewriter(List<ColumnRefOperator> correlationCols, OptimizerContext optimizerContext) {
        correlationColSet = new ColumnRefSet(correlationCols);
        this.optimizerContext = optimizerContext;
        exprToColumnRefMap = Maps.newHashMap();
    }

    public ScalarOperator rewrite(ScalarOperator correlationPredicate) {
        if (correlationPredicate == null) {
            return null;
        }
        return correlationPredicate.clone().accept(this, null);
    }

    @Override
    public ScalarOperator visit(ScalarOperator operator, Void context) {
        ColumnRefSet usedColumns = operator.getUsedColumns();

        if (correlationColSet.containsAll(usedColumns)) {
            return operator;
        }

        if (!correlationColSet.isIntersect(usedColumns)) {
            return addExprToColumnRefMap(operator);
        }

        for (int i = 0; i < operator.getChildren().size(); i++) {
            ScalarOperator child = operator.getChild(i);
            operator.getChildren().set(i, child.accept(this, null));
        }

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

    /**
     * if exprToColumnRefMap doesn't contain operator means should create a new columnRef to replace this operator,
     * otherwise replace this operator with exist columnRef.
     *
     * @param operator
     * @return
     */
    private ScalarOperator addExprToColumnRefMap(ScalarOperator operator) {
        if (!exprToColumnRefMap.containsKey(operator)) {
            ColumnRefOperator columnRefOperator = createColumnRefOperator(operator);
            exprToColumnRefMap.put(operator, columnRefOperator);
            return columnRefOperator;
        } else {
            return exprToColumnRefMap.get(operator);
        }
    }

    private ColumnRefOperator createColumnRefOperator(ScalarOperator operator) {
        return optimizerContext.getColumnRefFactory().create(operator, operator.getType(), operator.isNullable());
    }
}
