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

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSetOperation;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;

public class InputDependenciesChecker implements PlanValidator.Checker {

    private static final String PREFIX = "Input dependency cols check failed.";
    private static final InputDependenciesChecker INSTANCE = new InputDependenciesChecker();

    private InputDependenciesChecker() {}

    public static InputDependenciesChecker getInstance() {
        return INSTANCE;
    }

    @Override
    public void validate(OptExpression physicalPlan, TaskContext taskContext) {
        Visitor visitor = new Visitor();
        visitor.visit(physicalPlan, null);
    }

    private static class Visitor extends OptExpressionVisitor<Void, Void> {

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            Operator operator = optExpression.getOp();
            if (optExpression.arity() == 0) {
                checkOptWithoutChild(optExpression);
            } else if (optExpression.arity() == 1) {
                checkOptExprWithOneChild(optExpression);
            } else if (operator instanceof LogicalJoinOperator || operator instanceof PhysicalJoinOperator) {
                checkJoinOpt(optExpression);
            } else if (operator instanceof LogicalSetOperator || operator instanceof PhysicalSetOperation) {
                checkSetOpt(optExpression);
            } else {
                for (OptExpression input : optExpression.getInputs()) {
                    visit(input, context);
                }
            }
            return context;
        }

        private void checkOptWithoutChild(OptExpression optExpression) {
            RowOutputInfo rowOutputInfo = optExpression.getRowOutputInfo();
            Operator operator = optExpression.getOp();
            if (operator instanceof LogicalScanOperator || operator instanceof PhysicalScanOperator
                    || operator instanceof LogicalValuesOperator || operator instanceof PhysicalValuesOperator) {
                ColumnRefSet inputCols = ColumnRefSet.createByIds(rowOutputInfo.getOriginalColOutputInfo().keySet());
                ColumnRefSet usedCols = new ColumnRefSet();
                for (ColumnOutputInfo col : rowOutputInfo.getColumnOutputInfo()) {
                    usedCols.union(col.getUsedColumns());
                }
                for (ColumnOutputInfo col : rowOutputInfo.getCommonColInfo()) {
                    usedCols.union(col.getUsedColumns());
                }

                for (ColumnOutputInfo col : rowOutputInfo.getCommonColInfo()) {
                    usedCols.except(col.getColumnRef().getUsedColumns());
                }
                checkInputCols(inputCols, usedCols, optExpression);
            }
        }

        private void checkOptExprWithOneChild(OptExpression optExpression) {
            visit(optExpression.inputAt(0), null);
            // LogicalCteConsumer's input col actually is from LogicalCteProducer not from its input
            // So we skip check it
            if (optExpression.getOp() instanceof LogicalCTEConsumeOperator) {
                return;
            }
            ColumnRefSet inputCols = optExpression.inputAt(0).getRowOutputInfo().getOutputColumnRefSet();
            ColumnRefSet usedCols = optExpression.getRowOutputInfo().getUsedColumnRefSet();
            checkInputCols(inputCols, usedCols, optExpression);
        }

        private void checkJoinOpt(OptExpression optExpression) {
            for (OptExpression input : optExpression.getInputs()) {
                visit(input, null);
            }
            ColumnRefSet inputCols = new ColumnRefSet();
            for (OptExpression input : optExpression.getInputs()) {
                inputCols.union(input.getRowOutputInfo().getOutputColumnRefSet());
            }
            ColumnRefSet usedCols = optExpression.getRowOutputInfo().getUsedColumnRefSet();
            if (optExpression.getOp() instanceof LogicalJoinOperator) {
                LogicalJoinOperator joinOperator = optExpression.getOp().cast();
                if (joinOperator.getOnPredicate() != null) {
                    usedCols.union(joinOperator.getOnPredicate().getUsedColumns());
                }
                if (joinOperator.getPredicate() != null) {
                    usedCols.union(joinOperator.getPredicate().getUsedColumns());
                }
            } else {
                PhysicalJoinOperator joinOperator = optExpression.getOp().cast();
                if (joinOperator.getOnPredicate() != null) {
                    usedCols.union(joinOperator.getOnPredicate().getUsedColumns());
                }
                if (joinOperator.getPredicate() != null) {
                    usedCols.union(joinOperator.getPredicate().getUsedColumns());
                }
            }
            checkInputCols(inputCols, usedCols, optExpression);
        }

        private void checkSetOpt(OptExpression optExpression) {
            for (OptExpression input : optExpression.getInputs()) {
                visit(input, null);
            }

            List<ColumnRefOperator> outputColumnRefs;
            List<List<ColumnRefOperator>> requiredChildOuputCols;
            if (optExpression.getOp() instanceof LogicalSetOperator) {
                LogicalSetOperator logicalSetOperator = (LogicalSetOperator) optExpression.getOp();
                outputColumnRefs = logicalSetOperator.getOutputColumnRefOp();
                requiredChildOuputCols = logicalSetOperator.getChildOutputColumns();
            } else {
                PhysicalSetOperation physicalSetOperation = (PhysicalSetOperation) optExpression.getOp();
                outputColumnRefs = physicalSetOperation.getOutputColumnRefOp();
                requiredChildOuputCols = physicalSetOperation.getChildOutputColumns();
            }
            checkChildNumberOfSet(optExpression.arity(), requiredChildOuputCols.size(), optExpression);
            for (int i = 0; i < optExpression.arity(); i++) {
                RowOutputInfo inputRow = optExpression.inputAt(i).getRowOutputInfo();
                ColumnRefSet inputCols = inputRow.getOutputColumnRefSet();
                for (int j = 0; j < outputColumnRefs.size(); j++) {
                    ColumnRefOperator requiredCol = requiredChildOuputCols.get(i).get(j);
                    checkInputCols(inputCols, requiredCol.getUsedColumns(), optExpression);
                    checkInputType(requiredCol, outputColumnRefs.get(j), optExpression);
                }
            }
        }

        private void checkInputCols(ColumnRefSet inputCols, ColumnRefSet usedCols, OptExpression optExpr) {
            ColumnRefSet missedCols = usedCols.clone();
            missedCols.except(inputCols);
            if (!missedCols.isEmpty()) {
                String message = String.format("Invalid plan:%s%s%s The required cols %s cannot obtain from input cols %s.",
                        System.lineSeparator(), optExpr.debugString(), PREFIX, missedCols, inputCols);
                throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
            }
        }

        private void checkInputType(ColumnRefOperator inputCol, ColumnRefOperator outputCol, OptExpression optExpression) {
            if (!outputCol.getType().isFullyCompatible(inputCol.getType())) {
                String message = String.format("Invalid plan:%s%s%s Type of output col %s is not fully compatible with " +
                                "type of input col %s.",
                        System.lineSeparator(), optExpression.debugString(), PREFIX, outputCol, inputCol);
                throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
            }
        }

        private void checkChildNumberOfSet(int inputSize, int requiredSize, OptExpression optExpression) {
            if (inputSize != requiredSize) {
                String message = String.format("Invalid plan:%s%s%s. The required number of children is %d but found %d.",
                        System.lineSeparator(), optExpression.debugString(), PREFIX, requiredSize, inputSize);
                throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
            }
        }
    }
}
