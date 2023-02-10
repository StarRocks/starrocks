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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
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
            LogicalOperator logicalOperator = (LogicalOperator) optExpression.getOp();
            ExpressionContext expressionContext = new ExpressionContext(optExpression);
            Map<ColumnRefOperator, ScalarOperator> projection = logicalOperator.getLineage(refFactory, expressionContext);
            lineage.putAll(projection);
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }
    }

    public static Map<ColumnRefOperator, ScalarOperator> getLineage(OptExpression expression,
                                                                    ColumnRefFactory refFactory) {
        LineageFactory factory = new LineageFactory(expression, refFactory);
        return factory.getLineage();
    }
}
