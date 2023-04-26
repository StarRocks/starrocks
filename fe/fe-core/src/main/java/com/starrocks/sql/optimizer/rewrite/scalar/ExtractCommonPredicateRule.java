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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.List;

public class ExtractCommonPredicateRule extends TopDownScalarOperatorRewriteRule {
    //
    // Extract Common Predicate
    // example:
    //            OR
    //          /    \
    //      AND         AND
    //     /   \       /  \
    // a = b   c = 1  a = b  d = 2
    //
    // After rule:
    //             AND
    //            /   \
    //         a = b   OR
    //                /  \
    //            c = 1  d = 2
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        List<ScalarOperator> orLists = Utils.extractDisjunctive(predicate);
        if (orLists.size() <= 1) {
            return predicate;
        }

        List<List<ScalarOperator>> orAndPredicates = Lists.newArrayList();

        for (ScalarOperator or : orLists) {
            orAndPredicates.add(Lists.newArrayList(Utils.extractConjuncts(or)));
        }

        // extract common predicate
        List<ScalarOperator> common = Lists.newArrayList(orAndPredicates.get(0));
        for (int i = 1; i < orAndPredicates.size(); i++) {
            common.retainAll(orAndPredicates.get(i));
        }

        if (common.isEmpty()) {
            return predicate;
        }

        for (List<ScalarOperator> andPredicates : orAndPredicates) {
            andPredicates.removeAll(common);

            // only contain common predicate, other predicates will invalid
            if (andPredicates.isEmpty()) {
                return Utils.compoundAnd(common);
            }
        }

        ScalarOperator newOr = null;
        for (List<ScalarOperator> andPredicates : orAndPredicates) {
            newOr = Utils.compoundOr(newOr, Utils.compoundAnd(andPredicates));
        }

        return Utils.compoundAnd(Utils.compoundAnd(common), newOr);
    }
}
