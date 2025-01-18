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

package com.starrocks.sql.optimizer.rule.transformation.materialization.compensation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;

import java.util.List;

import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_PARTITION_PRUNED;

/**
 * Compensate the scan operator with the partition compensation.
 */
public class OptCompensator extends OptExpressionVisitor<OptExpression, Void> {
    private final OptimizerContext optimizerContext;
    private final MaterializedView mv;
    private final MVCompensation mvCompensation;

    // for olap table
    public OptCompensator(OptimizerContext optimizerContext,
                          MaterializedView mv,
                          MVCompensation mvCompensation) {
        this.optimizerContext = optimizerContext;
        this.mv = mv;
        this.mvCompensation = mvCompensation;
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
        LogicalScanOperator scanOperator = optExpression.getOp().cast();
        Table refBaseTable = scanOperator.getTable();
        if (!mvCompensation.isTableNeedCompensate(refBaseTable)) {
            return optExpression;
        }
        TableCompensation compensation = mvCompensation.getTableCompensation(refBaseTable);
        if (compensation.isNoCompensate()) {
            return optExpression;
        }
        if (MvPartitionCompensator.isSupportPartitionCompensate(refBaseTable)) {
            LogicalScanOperator newScanOperator = compensation.compensate(optimizerContext, mv, scanOperator);
            Preconditions.checkArgument(newScanOperator != null);
            // reset the partition prune flag to be pruned again.
            newScanOperator.resetOpRuleBit(OP_PARTITION_PRUNED);
            return OptExpression.create(newScanOperator);
        } else {
            return optExpression;
        }
    }

    @Override
    public OptExpression visit(OptExpression optExpression, Void context) {
        List<OptExpression> children = Lists.newArrayList();
        for (int i = 0; i < optExpression.arity(); ++i) {
            children.add(optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), null));
        }
        return OptExpression.create(optExpression.getOp(), children);
    }

    /**
     * Get the compensation plan for the mv.
     * @param mv the mv to compensate
     * @param mvCompensation the compensations for the mv, including ref base table's compensations
     * @param optExpression query plan with the ref base table
     * @return the compensation plan for the mv
     */
    public static OptExpression getMVCompensatePlan(OptimizerContext optimizerContext,
                                                    MaterializedView mv,
                                                    MVCompensation mvCompensation,
                                                    OptExpression optExpression) {
        OptCompensator scanOperatorCompensator = new OptCompensator(optimizerContext, mv, mvCompensation);
        return optExpression.getOp().accept(scanOperatorCompensator, optExpression, null);
    }
}