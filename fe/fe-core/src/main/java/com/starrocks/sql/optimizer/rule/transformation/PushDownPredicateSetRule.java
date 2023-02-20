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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PushDownPredicateSetRule {
    static List<OptExpression> process(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOperator = (LogicalFilterOperator) input.getOp();

        OptExpression setOptExpression = input.getInputs().get(0);
        LogicalSetOperator setOperator = (LogicalSetOperator) setOptExpression.getOp();

        for (int setChildIdx = 0; setChildIdx < setOptExpression.getInputs().size(); ++setChildIdx) {
            Map<ColumnRefOperator, ScalarOperator> operatorMap = new HashMap<>();

            for (int i = 0; i < setOperator.getOutputColumnRefOp().size(); ++i) {
                /*
                 * getChildOutputColumns records the output list of child children.
                 * Need to use the column id of the child to replace the output columns of the set node
                 * in the process of pushing down the predicate.
                 */
                ColumnRefOperator c = setOperator.getChildOutputColumns().get(setChildIdx).get(i);
                operatorMap.put(setOperator.getOutputColumnRefOp().get(i), c);
            }

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(operatorMap);
            ScalarOperator rewriteExpr = rewriter.rewrite(filterOperator.getPredicate());

            OptExpression filterOpExpression =
                    OptExpression.create(new LogicalFilterOperator(rewriteExpr), setOptExpression.inputAt(setChildIdx));
            setOptExpression.setChild(setChildIdx, filterOpExpression);
        }

        return Lists.newArrayList(setOptExpression);
    }
}
