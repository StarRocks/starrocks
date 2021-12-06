// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.List;
import java.util.Set;

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
}
