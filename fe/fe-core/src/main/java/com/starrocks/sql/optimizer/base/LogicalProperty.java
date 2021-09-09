// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
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
    // The tablets num of left most scan node
    private int leftMostScanTabletsNum;
    // The max tablets num of scan nodes
    private boolean isExecuteInOneInstance;

    public ColumnRefSet getOutputColumns() {
        return outputColumns;
    }

    public int getLeftMostScanTabletsNum() {
        return leftMostScanTabletsNum;
    }

    public boolean isExecuteInOneInstance() {
        return isExecuteInOneInstance;
    }

    public void setLeftMostScanTabletsNum(int leftMostScanTabletsNum) {
        this.leftMostScanTabletsNum = leftMostScanTabletsNum;
    }

    public LogicalProperty() {
        this.outputColumns = new ColumnRefSet();
    }

    public LogicalProperty(ColumnRefSet outputColumns) {
        this.outputColumns = outputColumns;
    }

    public void derive(ExpressionContext expressionContext) {
        LogicalOperator op = (LogicalOperator) expressionContext.getOp();
        outputColumns = op.getOutputColumns(expressionContext);
        leftMostScanTabletsNum = op.accept(new LeftMostScanTabletsNumVisitor(), expressionContext);
        isExecuteInOneInstance = op.accept(new OneInstanceExecutorVisitor(), expressionContext);
    }

    static class LeftMostScanTabletsNumVisitor extends OperatorVisitor<Integer, ExpressionContext> {
        @Override
        public Integer visitOperator(Operator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() != 0);
            return context.getChildLeftMostScanTabletsNum(0);
        }

        @Override
        public Integer visitMockOperator(MockOperator node, ExpressionContext context) {
            return 1;
        }

        @Override
        public Integer visitLogicalTableScan(LogicalScanOperator node, ExpressionContext context) {
            if (node instanceof LogicalOlapScanOperator) {
                return ((LogicalOlapScanOperator) node).getSelectedTabletId().size();
            } else {
                // other scan operator, this is not 1 because avoid to generate 1 phase agg
                return 2;
            }
        }

        @Override
        public Integer visitLogicalValues(LogicalValuesOperator node, ExpressionContext context) {
            return 1;
        }

        @Override
        public Integer visitLogicalTableFunction(LogicalTableFunctionOperator node, ExpressionContext context) {
            return 1;
        }
    }

    static class OneInstanceExecutorVisitor extends OperatorVisitor<Boolean, ExpressionContext> {
        @Override
        public Boolean visitOperator(Operator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() != 0);
            return context.isExecuteInOneInstance(0);
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
                return node instanceof LogicalMysqlScanOperator;
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
    }
}
