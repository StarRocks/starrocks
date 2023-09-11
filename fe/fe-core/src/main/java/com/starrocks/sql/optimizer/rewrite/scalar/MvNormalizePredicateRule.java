// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MvNormalizePredicateRule extends NormalizePredicateRule {

    // should maintain sequence for case:
    // a like "%hello%" and (b * c = 100 or b * c = 200)
    // (b * c = 200 or b * c = 100) and a like "%hello%"
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        Comparator<ScalarOperator> byToString =
                (ScalarOperator o1, ScalarOperator o2) -> o1.toString().compareTo(o2.toString());
        Set<ScalarOperator> after = Sets.newTreeSet(byToString);
        if (predicate.isAnd()) {
            List<ScalarOperator> before = Utils.extractConjuncts(predicate);
            after.addAll(before);
            if (Lists.newArrayList(after).equals(before)) {
                return predicate;
            }
            return Utils.compoundAnd(Lists.newArrayList(after));
        } else if (predicate.isOr()) {
            List<ScalarOperator> before = Utils.extractDisjunctive(predicate);
            after.addAll(before);
            if (Lists.newArrayList(after).equals(before)) {
                return predicate;
            }
            return Utils.compoundOr(Lists.newArrayList(after));
        } else {
            // for not
            return predicate;
        }
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        List<ScalarOperator> rhs = predicate.getChildren().subList(1, predicate.getChildren().size());
        if (predicate.isSubquery()) {
            return predicate;
        }
        if (!rhs.stream().allMatch(ScalarOperator::isConstant)) {
            return predicate;
        }

        List<ScalarOperator> result = new ArrayList<>();
        ScalarOperator lhs = predicate.getChild(0);
        boolean isIn = !predicate.isNotIn();

        List<ScalarOperator> constants = predicate.getChildren().stream().skip(1).filter(ScalarOperator::isConstant)
                .collect(Collectors.toList());
        if (constants.size() == 1) {
            BinaryType op =
                    isIn ? BinaryType.EQ : BinaryType.NE;
            result.add(new BinaryPredicateOperator(op, lhs, constants.get(0)));
        } else if (constants.size() > 1024) {
            // add a size limit to protect in with large number of children
            return predicate;
        } else if (!constants.isEmpty()) {
            for (ScalarOperator constant : constants) {
                BinaryType op =
                        isIn ? BinaryType.EQ : BinaryType.NE;
                result.add(new BinaryPredicateOperator(op, lhs, constant));
            }
        }

        predicate.getChildren().stream().skip(1).filter(ScalarOperator::isVariable).forEach(child -> {
            BinaryPredicateOperator newOp;
            if (isIn) {
                newOp = new BinaryPredicateOperator(BinaryType.EQ, lhs, child);
            } else {
                newOp = new BinaryPredicateOperator(BinaryType.NE, lhs, child);
            }
            result.add(newOp);
        });

        return isIn ? Utils.compoundOr(result) : Utils.compoundAnd(result);
    }

    // NOTE: View-Delta Join may produce redundant compensation predicates as below.
    // eg:
    // A(pk: a1)  <-> B (pk: b1)
    // A(pk: a2)  <-> C (pk: c1)
    // Query: a1 = a2
    // view delta deduce: a1 = a2 , a1 = b1, a2 = c1
    // src equal classes  : a1, a2, b1, c1
    // dest equal classes :
    //            target1 : a1, b1
    //            target2 : a2, c1
    // For src equal classes and target1 equal classes, compensation predicates should be:
    //  query.a2 = target.a1 and c1 = b1
    // For src equal classes and target2 equal classes, compensation predicates should be:
    //  query.a1 = target.a2 and b1 = c1
    public static ScalarOperator pruneRedundantPredicates(ScalarOperator predicate) {
        List<ScalarOperator> predicates = Utils.extractConjuncts(predicate);
        List<ScalarOperator> prunedPredicates = pruneRedundantPredicates(predicates);
        if (predicates != null) {
            return Utils.compoundAnd(prunedPredicates);
        }
        return predicate;
    }

    public static List<ScalarOperator> pruneRedundantPredicates(List<ScalarOperator> scalarOperators) {
        List<ScalarOperator> prunedPredicates = pruneEqualBinaryPredicates(scalarOperators);
        if (prunedPredicates != null) {
            return prunedPredicates;
        }
        return scalarOperators;
    }

    // a = b & b = a => a = b
    private static List<ScalarOperator> pruneEqualBinaryPredicates(List<ScalarOperator> scalarOperators) {
        Map<ColumnRefOperator, ColumnRefOperator> visited = Maps.newHashMap();
        List<ScalarOperator> prunedPredicates = Lists.newArrayList();
        for (ScalarOperator scalarOperator : scalarOperators) {
            if (!(scalarOperator instanceof BinaryPredicateOperator)) {
                prunedPredicates.add(scalarOperator);
                continue;
            }
            BinaryPredicateOperator binaryPred = (BinaryPredicateOperator) scalarOperator;
            if (!binaryPred.getBinaryType().isEqual()) {
                prunedPredicates.add(scalarOperator);
                continue;
            }
            if (!binaryPred.getChild(0).isColumnRef() || !binaryPred.getChild(1).isColumnRef()) {
                prunedPredicates.add(scalarOperator);
                continue;
            }
            ColumnRefOperator col0 = (ColumnRefOperator) (binaryPred.getChild(0));
            ColumnRefOperator col1 = (ColumnRefOperator) (binaryPred.getChild(1));
            if (visited.containsKey(col0) && visited.get(col0).equals(col1) ||
                    visited.containsKey(col1) && visited.get(col1).equals(col0)) {
                continue;
            }
            prunedPredicates.add(scalarOperator);
            visited.put(col0, col1);
        }
        return prunedPredicates;
    }
}
