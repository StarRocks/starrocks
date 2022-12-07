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
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Set;

public class MergeTwoFiltersRule extends TransformationRule {
    public MergeTwoFiltersRule() {
        super(RuleType.TF_MERGE_TWO_FILTERS,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filter1 = (LogicalFilterOperator) input.getOp();
        OptExpression child = input.getInputs().get(0);
        LogicalFilterOperator filter2 = (LogicalFilterOperator) child.getOp();

        Set<ScalarOperator> set = Sets.newLinkedHashSet();
        set.addAll(Utils.extractConjuncts(filter1.getPredicate()));
        set.addAll(Utils.extractConjuncts(filter2.getPredicate()));
        LogicalFilterOperator newFilter = new LogicalFilterOperator(Utils.compoundAnd(Lists.newArrayList(set)));

        return Lists.newArrayList(OptExpression.create(newFilter, child.getInputs()));
    }
}
