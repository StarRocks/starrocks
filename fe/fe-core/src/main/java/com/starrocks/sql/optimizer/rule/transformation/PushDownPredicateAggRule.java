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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Iterator;
import java.util.List;

public class PushDownPredicateAggRule extends TransformationRule {
    public PushDownPredicateAggRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_AGG,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator logicalFilterOperator = (LogicalFilterOperator) input.getOp();
        List<ScalarOperator> filters = Utils.extractConjuncts(logicalFilterOperator.getPredicate());

        LogicalAggregationOperator logicalAggOperator = (LogicalAggregationOperator) input.inputAt(0).getOp();
        List<ColumnRefOperator> groupColumns = logicalAggOperator.getGroupingKeys();

        List<ScalarOperator> pushDownPredicates = Lists.newArrayList();

        for (Iterator<ScalarOperator> iter = filters.iterator(); iter.hasNext(); ) {
            ScalarOperator scalar = iter.next();
            List<ColumnRefOperator> columns = Utils.extractColumnRef(scalar);

            // push down predicate
            if (groupColumns.containsAll(columns) && !columns.isEmpty()) {
                // remove from filter
                iter.remove();
                // add to push down predicates
                pushDownPredicates.add(scalar);
            }
        }

        // merge filter
        filters.add(logicalAggOperator.getPredicate());
        input.setChild(0, OptExpression.create(new LogicalAggregationOperator.Builder().withOperator(logicalAggOperator)
                        .setPredicate(Utils.compoundAnd(filters))
                        .build(),
                input.inputAt(0).getInputs()));

        // push down
        if (pushDownPredicates.size() > 0) {
            LogicalFilterOperator newFilter = new LogicalFilterOperator(Utils.compoundAnd(pushDownPredicates));
            OptExpression oe = new OptExpression(newFilter);
            oe.getInputs().addAll(input.inputAt(0).getInputs());

            input.inputAt(0).getInputs().clear();
            input.inputAt(0).getInputs().add(oe);
        }

        return Lists.newArrayList(input.inputAt(0));
    }
}
