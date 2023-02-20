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


package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;

public class LogicalProperty implements Property {
    // Operator's output columns
    private ColumnRefSet outputColumns;

    // The flag for execute upon less than or equal one tablet
    private boolean isExecuteInOneTablet;

    public ColumnRefSet getOutputColumns() {
        return outputColumns;
    }

    public void setOutputColumns(ColumnRefSet outputColumns) {
        this.outputColumns = outputColumns;
    }

    public boolean isExecuteInOneTablet() {
        return isExecuteInOneTablet;
    }

    public LogicalProperty() {
        this.outputColumns = new ColumnRefSet();
    }

    public LogicalProperty(ColumnRefSet outputColumns) {
        this.outputColumns = outputColumns;
    }

    public LogicalProperty(LogicalProperty other) {
        outputColumns = other.outputColumns.clone();
        isExecuteInOneTablet = other.isExecuteInOneTablet;
    }

    public void derive(ExpressionContext expressionContext) {
        LogicalOperator op = (LogicalOperator) expressionContext.getOp();
        outputColumns = op.getOutputColumns(expressionContext);
        isExecuteInOneTablet = op.accept(new OneTabletExecutorVisitor(), expressionContext);
    }

    static class OneTabletExecutorVisitor extends OperatorVisitor<Boolean, ExpressionContext> {
        @Override
        public Boolean visitOperator(Operator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() != 0);
            return context.isExecuteInOneTablet(0);
        }

        @Override
        public Boolean visitMockOperator(MockOperator node, ExpressionContext context) {
            return true;
        }

        @Override
        public Boolean visitLogicalTableScan(LogicalScanOperator node, ExpressionContext context) {
            if (node instanceof LogicalOlapScanOperator) {
                return ((LogicalOlapScanOperator) node).getSelectedTabletId().size() <= 1;
            } else {
                return node instanceof LogicalMysqlScanOperator || node instanceof LogicalJDBCScanOperator;
            }
        }

        @Override
        public Boolean visitLogicalValues(LogicalValuesOperator node, ExpressionContext context) {
            return true;
        }

        @Override
        public Boolean visitLogicalJoin(LogicalJoinOperator node, ExpressionContext context) {
            return false;
        }

        @Override
        public Boolean visitLogicalUnion(LogicalUnionOperator node, ExpressionContext context) {
            return false;
        }

        @Override
        public Boolean visitLogicalExcept(LogicalExceptOperator node, ExpressionContext context) {
            return false;
        }

        @Override
        public Boolean visitLogicalIntersect(LogicalIntersectOperator node, ExpressionContext context) {
            return false;
        }

        @Override
        public Boolean visitLogicalTableFunction(LogicalTableFunctionOperator node, ExpressionContext context) {
            return false;
        }

        @Override
        public Boolean visitLogicalCTEAnchor(LogicalCTEAnchorOperator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() == 2);
            return context.isExecuteInOneTablet(1);
        }

        @Override
        public Boolean visitLogicalCTEConsume(LogicalCTEConsumeOperator node, ExpressionContext context) {
            return false;
        }
    }
}
