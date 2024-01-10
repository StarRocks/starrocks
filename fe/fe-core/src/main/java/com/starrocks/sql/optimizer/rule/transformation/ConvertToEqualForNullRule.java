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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Optional;
import java.util.Set;

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
            newConjuncts.add(convertToEqualForNull(conjunct).orElse(conjunct));
        }
        OptExpression newJoinOpt = OptExpression.create(new LogicalJoinOperator.Builder().withOperator(joinOperator)
                .setOnPredicate(Utils.compoundAnd(newConjuncts))
                .build(), input.getInputs());

        return Lists.newArrayList(newJoinOpt);
    }

    private Optional<ScalarOperator> convertToEqualForNull(ScalarOperator scalarOperator) {
        if (!(scalarOperator instanceof CompoundPredicateOperator)) {
            return Optional.empty();
        }

        CompoundPredicateOperator compoundOp = (CompoundPredicateOperator) scalarOperator;
        if (!compoundOp.isOr()) {
            return Optional.empty();
        }

        if (compoundOp.getChild(0) instanceof CompoundPredicateOperator
                || compoundOp.getChild(1) instanceof BinaryPredicateOperator) {
            compoundOp = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                    compoundOp.getChild(1), compoundOp.getChild(0));
        }

        if (!(compoundOp.getChild(0) instanceof BinaryPredicateOperator)
                || !(compoundOp.getChild(1) instanceof CompoundPredicateOperator)) {
            return Optional.empty();
        }

        BinaryPredicateOperator left = (BinaryPredicateOperator) compoundOp.getChild(0);
        CompoundPredicateOperator right = (CompoundPredicateOperator) compoundOp.getChild(1);
        if (!left.getBinaryType().isEqual() || !right.isAnd()) {
            return Optional.empty();
        }

        if (!(right.getChild(0) instanceof IsNullPredicateOperator)
                || !(right.getChild(1) instanceof IsNullPredicateOperator)) {
            return Optional.empty();
        }

        IsNullPredicateOperator isNullLeft = (IsNullPredicateOperator) right.getChild(0);
        IsNullPredicateOperator isNullRight = (IsNullPredicateOperator) right.getChild(1);
        if (isNullLeft.isNotNull() || isNullRight.isNotNull()) {
            return Optional.empty();
        }

        Set<ScalarOperator> leftChildren = ImmutableSet.of(left.getChild(0), left.getChild(1));
        Set<ScalarOperator> rightChildren = ImmutableSet.of(isNullLeft.getChild(0), isNullRight.getChild(0));

        if (leftChildren.equals(rightChildren)) {
            return Optional.of(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, left.getChild(0), left.getChild(1)));
        } else {
            return Optional.empty();
        }
    }
}
