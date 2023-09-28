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
package com.starrocks.sql.optimizer.validate;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnReuseChecker implements PlanValidator.Checker {

    private static final ColumnReuseChecker INSTANCE = new ColumnReuseChecker();

    private ColumnReuseChecker() {
    }

    public static ColumnReuseChecker getInstance() {
        return INSTANCE;
    }

    @Override
    public void validate(OptExpression physicalPlan, TaskContext taskContext) {
        Visitor visitor = new Visitor();
        physicalPlan.getOp().accept(visitor, physicalPlan, null);
    }

    private static class Visitor extends OptExpressionVisitor<Void, Void> {

        private void checkReusedColumnRef(Operator op, Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
            if (columnRefMap == null) {
                return;
            }
            List<ScalarOperator> columnRefs = columnRefMap.values().stream()
                    .filter(ScalarOperator::isColumnRef)
                    .collect(Collectors.toList());
            List<ScalarOperator> uniqueColumnRefs = columnRefs.stream()
                    .distinct()
                    .collect(Collectors.toList());
            Preconditions.checkArgument(columnRefs.size() == uniqueColumnRefs.size(),
                    "Operator %s has reused column.", op.getOpType());
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            Operator op = optExpression.getOp();
            Projection projection = op.getProjection();
            if (projection != null) {
                checkReusedColumnRef(op, projection.getColumnRefMap());
                checkReusedColumnRef(op, projection.getCommonSubOperatorMap());
            }
            for (OptExpression input : optExpression.getInputs()) {
                Operator operator = input.getOp();
                operator.accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitLogicalProject(OptExpression optExpression, Void context) {
            LogicalProjectOperator projectOperator = optExpression.getOp().cast();
            checkReusedColumnRef(projectOperator, projectOperator.getColumnRefMap());
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalProject(OptExpression optExpression, Void context) {
            PhysicalProjectOperator projectOperator = optExpression.getOp().cast();
            checkReusedColumnRef(projectOperator, projectOperator.getColumnRefMap());
            return visit(optExpression, context);
        }
    }
}