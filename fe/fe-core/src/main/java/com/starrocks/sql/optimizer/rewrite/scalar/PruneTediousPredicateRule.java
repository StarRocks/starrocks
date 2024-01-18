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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PruneTediousPredicateRule extends OnlyOnceScalarOperatorRewriteRule {

    private PruneTediousPredicateRule() {
    }

    // Prune tedious predicates introduced by case-when simplification, the tedious predicates are in two forms
    // as follows:
    // 1. p OR if(q, NULL, FALSE) => p
    // 2. p AND p is NOT NULL => p
<<<<<<< HEAD
=======
    // 3. if(p, TRUE, NULL) => q
>>>>>>> 2.5.18
    // NOTICE!!! this pruning operations can only be applied to predicates in top-down style, it can not be applied
    // common expressions, for examples:
    // 1. select p or if (q, NULL, FALSE) from t;
    // 2. select * from t where (p and p is NOT NULL) is NULL;
    private static class Visitor extends ScalarOperatorVisitor<Optional<ScalarOperator>, Void> {

        @Override
        public Optional<ScalarOperator> visit(ScalarOperator scalarOperator, Void context) {
            return Optional.empty();
        }

<<<<<<< HEAD
        public static boolean outputOnlyNullOrFalse(ScalarOperator p) {
=======
        private static boolean outputOnlyNullOrFalse(ScalarOperator p) {
>>>>>>> 2.5.18
            if (!(p instanceof CallOperator)) {
                return false;
            }
            CallOperator call = (CallOperator) p;
            if (!call.getFnName().equals(FunctionSet.IF)) {
                return false;
            }
            return call.getChildren().stream().skip(1).allMatch(ScalarOperator::isConstantNullOrFalse);
        }

        @Override
        public Optional<ScalarOperator> visitCall(CallOperator call, Void context) {
            if (outputOnlyNullOrFalse(call)) {
                return Optional.of(ConstantOperator.FALSE);
<<<<<<< HEAD
=======
            }

            if (call.getFnName().equals(FunctionSet.IF)
                    && call.getChild(1).equals(ConstantOperator.TRUE)
                    && (call.getChild(2).equals(ConstantOperator.NULL) ||
                    call.getChild(2).equals(ConstantOperator.FALSE))) {
                return Optional.of(call.getChild(0));
>>>>>>> 2.5.18
            } else {
                return Optional.empty();
            }
        }

        @Override
        public Optional<ScalarOperator> visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (predicate.isAnd()) {
                List<ScalarOperator> conjuncts = predicate.getChildren();
                if (conjuncts.stream().anyMatch(Visitor::outputOnlyNullOrFalse)) {
                    return Optional.of(ConstantOperator.FALSE);
                }
<<<<<<< HEAD
=======
                conjuncts = conjuncts.stream().map(p -> p.accept(this, context).orElse(p))
                        .collect(Collectors.toList());
>>>>>>> 2.5.18
                return Optional.of(Utils.compoundAnd(conjuncts));
            } else if (predicate.isOr()) {
                List<ScalarOperator> disjuncts = predicate.getChildren().stream()
                        .filter(p -> !outputOnlyNullOrFalse(p)).collect(Collectors.toList());
                if (disjuncts.isEmpty()) {
                    return Optional.of(ConstantOperator.FALSE);
                } else if (disjuncts.size() > 1) {
                    return Optional.of(Utils.compoundOr(disjuncts));
                } else {
                    List<ScalarOperator> conjuncts = Utils.extractConjuncts(disjuncts.get(0)).stream()
                            .map(p -> p.accept(this, context).orElse(p)).collect(Collectors.toList());
                    return Optional.of(Utils.compoundAnd(conjuncts));
                }
            } else {
                return Optional.empty();
            }
        }
    }

    private static final Visitor TEDIOUS_PREDICATE_PRUNER = new Visitor();

    public static final PruneTediousPredicateRule INSTANCE = new PruneTediousPredicateRule();

    @Override
    public ScalarOperator apply(ScalarOperator root, ScalarOperatorRewriteContext context) {
        if (root == null) {
            return null;
        }
        return Utils.compoundAnd(Utils.extractConjuncts(root)).accept(TEDIOUS_PREDICATE_PRUNER, null).orElse(root);
    }
}
