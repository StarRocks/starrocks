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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NormalizePredicateRule extends BottomUpScalarOperatorRewriteRule {

    //
    // Normalize Binary Predicate
    //
    // example:
    //        Binary(=)
    //        /      \
    //    a(int)   b(column)
    //
    // After rule:
    //        Binary(=)
    //        /      \
    //  b(column)   a(int)
    //
    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (predicate.getChild(0).isVariable()) {
            return predicate;
        }

        if (predicate.getChild(1).isConstant()) {
            return predicate;
        }

        ScalarOperator result = predicate.commutative();
        Preconditions.checkState(!(result.getChild(0).isConstant() && result.getChild(1).isVariable()),
                "Normalized predicate error: " + result);
        return result;
    }

    //
    // Normalize Between Predicate
    // example:
    //          Between
    //        /    |    \
    //      col   "a"     "b"
    //
    // After rule:
    //                 AND
    //                /   \
    //               /     \
    //       Binary(>)      Binary(<)
    //        /    \         /    \
    //      col      "a"   col      "b"
    //
    @Override
    public ScalarOperator visitBetweenPredicate(BetweenPredicateOperator predicate,
                                                ScalarOperatorRewriteContext context) {
        if (predicate.isNotBetween()) {
            ScalarOperator lower =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, predicate.getChild(0),
                            predicate.getChild(1));

            ScalarOperator upper =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, predicate.getChild(0),
                            predicate.getChild(2));

            return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, lower, upper);
        } else {
            ScalarOperator lower =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, predicate.getChild(0),
                            predicate.getChild(1));

            ScalarOperator upper =
                    new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, predicate.getChild(0),
                            predicate.getChild(2));

            return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, lower, upper);
        }
    }

    // Remove repeat predicate
    // example:
    //           AND
    //        /        \
    //      AND         AND
    //     /   \       /  \
    // a = b   a = b  a = b  a = b
    //
    // After rule:
    //            a = b
    //
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {

        if (predicate.isAnd()) {
            Set<ScalarOperator> after = Sets.newLinkedHashSet();
            List<ScalarOperator> before = Utils.extractConjuncts(predicate);

            after.addAll(before);
            if (after.size() != before.size()) {
                return Utils.compoundAnd(Lists.newArrayList(after));
            }
        } else if (predicate.isOr()) {
            Set<ScalarOperator> after = Sets.newLinkedHashSet();
            List<ScalarOperator> before = Utils.extractDisjunctive(predicate);

            after.addAll(before);

            if (after.size() != before.size()) {
                return Utils.compoundOr(Lists.newArrayList(after));
            }
        }

        return predicate;
    }

    /*
     * Rewrite column ref into comparison predicate *
     * Before
     * example:
     *         IN
     *        / | \
     * left  1  a  b
     * After rule:
     * left = 1 OR left = a OR left = b
     */
    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        List<ScalarOperator> rhs = predicate.getChildren().subList(1, predicate.getChildren().size());
        if (predicate.isSubquery()) {
            return predicate;
        }
        if (rhs.stream().allMatch(ScalarOperator::isConstant)) {
            return predicate;
        }

        List<ScalarOperator> result = new ArrayList<>();
        ScalarOperator lhs = predicate.getChild(0);
        boolean isIn = !predicate.isNotIn();

        List<ScalarOperator> constants = predicate.getChildren().stream().skip(1).filter(ScalarOperator::isConstant)
                .collect(Collectors.toList());
        if (constants.size() == 1) {
            BinaryPredicateOperator.BinaryType op =
                    isIn ? BinaryPredicateOperator.BinaryType.EQ : BinaryPredicateOperator.BinaryType.NE;
            result.add(new BinaryPredicateOperator(op, lhs, constants.get(0)));
        } else if (!constants.isEmpty()) {
            constants.add(0, lhs);
            result.add(new InPredicateOperator(predicate.isNotIn(), constants));
        }

        predicate.getChildren().stream().skip(1).filter(ScalarOperator::isVariable).forEach(child -> {
            BinaryPredicateOperator newOp;
            if (isIn) {
                newOp = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, lhs, child);
            } else {
                newOp = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.NE, lhs, child);
            }
            result.add(newOp);
        });

        return isIn ? Utils.compoundOr(result) : Utils.compoundAnd(result);
    }
}
