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
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class ConvertToEqualForNullRule extends TransformationRule {
    public ConvertToEqualForNullRule() {
        super(RuleType.TF_CONVERT_TO_EQUAL_FOR_NULL_RULE, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        ScalarOperator onPredicate = joinOperator.getOnPredicate();
        return onPredicate != null;

    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        ScalarOperator onPredicate = joinOperator.getOnPredicate();
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(onPredicate);
        List<ScalarOperator> newConjuncts = Lists.newArrayList();
        for (ScalarOperator conjunct : conjuncts) {
            if (conjunct instanceof CompoundPredicateOperator) {
                CompoundPredicateOperator compoundOp = (CompoundPredicateOperator) conjunct;
                if (!compoundOp.isOr()) {
                    newConjuncts.add(conjunct);
                } else {

                }
                ScalarOperator left = compoundOp.getChild(0);
                ScalarOperator right = compoundOp.getChild(1);
                if (left instanceof BinaryPredicateOperator && right instanceof CompoundPredicateOperator) {
                    BinaryPredicateOperator leftOp = (BinaryPredicateOperator) left;
                    CompoundPredicateOperator rightOp = (CompoundPredicateOperator) right;
                    if (leftOp.getBinaryType().isEqual() && rightOp.isOr()) {

                    }
                }
            }
        }

        return conjuncts.stream().anyMatch( e -> e instanceof CompoundPredicateOperator
                && ((CompoundPredicateOperator) e).isOr());
    }
}
