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

package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;

public class BestIndexRewriter extends OptExpressionVisitor<OptExpression, Long> {
    LogicalOlapScanOperator scanOperator;

    public BestIndexRewriter(LogicalOlapScanOperator scanOperator) {
        this.scanOperator = scanOperator;
    }

    public OptExpression rewrite(OptExpression optExpression, Long context) {
        return optExpression.getOp().accept(this, optExpression, context);
    }

    @Override
    public OptExpression visit(OptExpression optExpression, Long context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        return OptExpression.create(optExpression.getOp(), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression, Long bestIndex) {
        if (!OperatorType.LOGICAL_OLAP_SCAN.equals(optExpression.getOp().getOpType())) {
            return optExpression;
        }

        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();

        if (olapScanOperator.equals(scanOperator)) {
            LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator(
                    olapScanOperator.getTable(),
                    olapScanOperator.getColRefToColumnMetaMap(),
                    olapScanOperator.getColumnMetaToColRefMap(),
                    olapScanOperator.getDistributionSpec(),
                    olapScanOperator.getLimit(),
                    olapScanOperator.getPredicate(),
                    bestIndex,
                    olapScanOperator.getSelectedPartitionId(),
                    olapScanOperator.getPartitionNames(),
                    olapScanOperator.getSelectedTabletId(),
                    olapScanOperator.getHintsTabletIds());

            optExpression = OptExpression.create(newScanOperator);
        }
        return optExpression;
    }
}
