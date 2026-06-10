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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Set;

// push down knn query into the lance scan node
public class PushDownKNNLanceScanRule extends TransformationRule {

    static final Set<String> SUPPORTED_DISTANCE_FUNCTIONS = Set.of(
            FunctionSet.COSINE_SIMILARITY,
            FunctionSet.L2_DISTANCE
    );

    public PushDownKNNLanceScanRule() {
        super(RuleType.TF_PUSH_DOWN_TOPN_SCAN, Pattern.create(OperatorType.LOGICAL_TOPN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_LANCE_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOperator = (LogicalTopNOperator) input.getOp();
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getInputs().get(0).getOp();
        // knn only allows sorting in ascending order of vector distance
        if (topNOperator.getOrderByElements().size() != 1) {
            return false;
        }
        Ordering ordering = topNOperator.getOrderByElements().get(0);
        if (!ordering.isAscending()) {
            return false;
        }
        Projection projection = scanOperator.getProjection();
        if (!projection.getColumnRefMap().containsKey(ordering.getColumnRef())) {
            return false;
        }
        ScalarOperator scalarOperator = projection.getColumnRefMap().get(ordering.getColumnRef());
        if (!(scalarOperator instanceof CallOperator)) {
            return false;
        }
        String fnName = ((CallOperator) scalarOperator).getFnName();
        if (!SUPPORTED_DISTANCE_FUNCTIONS.contains(fnName)) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // The lance scan node cannot guarantee the strict execution of topN, so we need to retain another topN node
        LogicalTopNOperator topNOperator = (LogicalTopNOperator) input.getOp();
        LogicalLanceScanOperator scanOperator = (LogicalLanceScanOperator) input.getInputs().get(0).getOp();
        LogicalLanceScanOperator.Builder builder = new LogicalLanceScanOperator.Builder();
        LogicalLanceScanOperator newScanOperator = builder.withOperator(scanOperator)
                .setOrderByElements(topNOperator.getOrderByElements())
                .setLimit(topNOperator.getLimit())
                .build();
        OptExpression result = OptExpression.create(topNOperator, List.of(OptExpression.create(newScanOperator)));
        return List.of(result);
    }
}
