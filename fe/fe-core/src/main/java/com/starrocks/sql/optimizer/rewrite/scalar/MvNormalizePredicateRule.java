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

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
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
    // Comparator to normalize predicates, only use scalar operators' string to compare.
    private static final Comparator<ScalarOperator> SCALAR_OPERATOR_COMPARATOR = new Comparator<ScalarOperator>() {
        @Override
        public int compare(ScalarOperator o1, ScalarOperator o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return -1;
            } else if (o2 == null) {
                return 1;
            } else {
                return o1.toString().compareTo(o2.toString());
            }
        }
    };


    // Comparator to normalize predicates, only use scalar operators' string to compare.
    private static final Comparator<ScalarOperator> SCALAR_OPERATOR_COMPARATOR_IGNORE_COLUMN_ID =
<<<<<<< HEAD
            new Comparator<ScalarOperator>() {
                @Override
                public int compare(ScalarOperator o1, ScalarOperator o2) {
                    if (o1 == null && o2 == null) {
                        return 0;
                    } else if (o1 == null) {
                        return -1;
                    } else if (o2 == null) {
                        return 1;
                    } else {
                        String s1 = o1.toString();
                        String s2 = o2.toString();
                        String n1 = s1.replaceAll("\\d: ", "");
                        String n2 = s2.replaceAll("\\d: ", "");
=======
            (o1, o2) -> {
                if (o1 == null && o2 == null) {
                    return 0;
                } else if (o1 == null) {
                    return -1;
                } else if (o2 == null) {
                    return 1;
                } else {
                    if (o1.isColumnRef() && o2.isColumnRef()) {
                        ColumnRefOperator c1 = (ColumnRefOperator) o1;
                        ColumnRefOperator c2 = (ColumnRefOperator) o2;
                        int ret = c1.getName().compareTo(c2.getName());
                        if (ret != 0) {
                            return ret;
                        }
                        return Integer.compare(c1.getId(), c2.getId());
                    } else {
                        String s1 = o1.toString();
                        String s2 = o2.toString();
                        String n1 = s1.replaceAll("\\d+: ", "");
                        String n2 = s2.replaceAll("\\d+: ", "");
>>>>>>> 097abdd1eb ([BugFix] Make unique/foreign key constraints case insensitive (#33902))
                        int ret = n1.compareTo(n2);
                        return (ret == 0) ? s1.compareTo(s2) : ret;
                    }
                }
            };

    // should maintain sequence for case:
    // a like "%hello%" and (b * c = 100 or b * c = 200)
    // (b * c = 200 or b * c = 100) and a like "%hello%"
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        Set<ScalarOperator> after = Sets.newTreeSet(SCALAR_OPERATOR_COMPARATOR);
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
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        ScalarOperator l = predicate.getChild(0);
        ScalarOperator r = predicate.getChild(1);
        if (l.isVariable() && r.isVariable()) {
            // `a < b` is equal to `b > a`, but here we all normalized it into a < b for better rewrite.
            if (SCALAR_OPERATOR_COMPARATOR_IGNORE_COLUMN_ID.compare(l, r) <= 0) {
                return predicate;
            }
            ScalarOperator result = predicate.commutative();
            Preconditions.checkState(SCALAR_OPERATOR_COMPARATOR_IGNORE_COLUMN_ID
                    .compare(result.getChild(0), result.getChild(1)) <= 0);
            return result;
        } else {
            return  super.visitBinaryPredicate(predicate, context);
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
