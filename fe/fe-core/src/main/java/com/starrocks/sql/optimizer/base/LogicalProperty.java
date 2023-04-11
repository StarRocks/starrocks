// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;
import com.starrocks.common.FeConstants;
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
    // The tablets num of left most scan node
    private int leftMostScanTabletsNum;
    // The flag for execute upon less than or equal one tablet
    private boolean isExecuteInOneTablet;

    public ColumnRefSet getOutputColumns() {
        return outputColumns;
    }

    public void setOutputColumns(ColumnRefSet outputColumns) {
        this.outputColumns = outputColumns;
    }

    public int getLeftMostScanTabletsNum() {
        return leftMostScanTabletsNum;
    }

    public boolean isExecuteInOneTablet() {
        return isExecuteInOneTablet;
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

    public LogicalProperty(LogicalProperty other) {
        outputColumns = other.outputColumns.clone();
        leftMostScanTabletsNum = other.leftMostScanTabletsNum;
        isExecuteInOneTablet = other.isExecuteInOneTablet;
    }

    public void derive(ExpressionContext expressionContext) {
        LogicalOperator op = (LogicalOperator) expressionContext.getOp();
        outputColumns = op.getOutputColumns(expressionContext);
        leftMostScanTabletsNum = op.accept(new LeftMostScanTabletsNumVisitor(), expressionContext);
        isExecuteInOneTablet = op.accept(new OneTabletExecutorVisitor(), expressionContext);
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
                // It's very hard to estimate how many tablets scanned by this operator,
                // because some operator even does not have the concept of tablets.
                // The value should not be too low, otherwise it will make cost optimizer to underestimate the cost of broadcast.
                // A thing to be noted that, this tablet number is better not to be 1, to avoid generate 1 phase agg.
                return FeConstants.default_tablet_number;
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

        @Override
        public Integer visitLogicalCTEAnchor(LogicalCTEAnchorOperator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() == 2);
            return context.getChildLeftMostScanTabletsNum(1);
        }

        @Override
        public Integer visitLogicalCTEConsume(LogicalCTEConsumeOperator node, ExpressionContext context) {
            return 2;
        }
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
