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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.stream.Collectors;

public class PredicateSplit {
    // column equality predicates conjuncts
    // a column equality predicate of the form (Ti.Cp =Tj.Cq)
    private final ScalarOperator equalPredicates;
    // range predicates conjuncts
    // a range predicate is any atomic predicate of the form (Ti.Cp op c)
    // where c is a constant and op is one of the operators “<”, “≤”, “=”, “≥”, “>”
    private final ScalarOperator rangePredicates;
    // residual predicates conjuncts
    // residual predicates containing all conjuncts except the two types above
    // eg: Ti.Cp like "%abc%"
    private final ScalarOperator residualPredicates;

    private PredicateSplit(ScalarOperator equalPredicates,
                           ScalarOperator rangePredicates,
                           ScalarOperator residualPredicates) {
        this.equalPredicates = equalPredicates;
        this.rangePredicates = rangePredicates;
        this.residualPredicates = residualPredicates;
    }

    public static PredicateSplit of(ScalarOperator equalPredicates,
                                    ScalarOperator rangePredicates,
                                    ScalarOperator residualPredicates) {
        return new PredicateSplit(equalPredicates, rangePredicates, residualPredicates);
    }

    public ScalarOperator getEqualPredicates() {
        return equalPredicates;
    }

    public ScalarOperator getRangePredicates() {
        return rangePredicates;
    }

    public ScalarOperator getResidualPredicates() {
        return residualPredicates;
    }

    public ScalarOperator toScalarOperator() {
        return Utils.compoundAnd(equalPredicates, rangePredicates, residualPredicates);
    }

    public List<ScalarOperator> getPredicates() {
        return Lists.newArrayList(equalPredicates, rangePredicates, residualPredicates);
    }

    private static ScalarOperator filterPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        List<ScalarOperator> filterPredicate = Utils.extractConjuncts(predicate)
                .stream()
                .filter(x -> !x.isConstantTrue())
                .collect(Collectors.toList());
        return Utils.compoundAnd(filterPredicate);
    }

    // split predicate into three parts: equal columns predicates, range predicates, and residual predicates
    public static PredicateSplit splitPredicate(ScalarOperator predicate) {
        ScalarOperator normalPredicate = filterPredicate(predicate);
        if (normalPredicate == null) {
            return PredicateSplit.of(null, null, null);
        }

        PredicateExtractor extractor = new PredicateExtractor();
        RangePredicate rangePredicate =
                normalPredicate.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
        ScalarOperator equalityConjunct = Utils.compoundAnd(extractor.getColumnEqualityPredicates());
        ScalarOperator rangeConjunct = null;
        ScalarOperator residualConjunct = Utils.compoundAnd(extractor.getResidualPredicates());
        if (rangePredicate != null) {
            // convert rangePredicate to rangeConjunct
            rangeConjunct = rangePredicate.toScalarOperator();
        } else if (extractor.getColumnEqualityPredicates().isEmpty() && extractor.getResidualPredicates().isEmpty()) {
            residualConjunct = Utils.compoundAnd(residualConjunct, normalPredicate);
        }
        return PredicateSplit.of(equalityConjunct, rangeConjunct, residualConjunct);
    }

    @Override
    public String toString() {
        return String.format("equalPredicates=%s, rangePredicates=%s, residualPredicates=%s",
                equalPredicates == null ? "" : equalPredicates,
                rangePredicates == null ? "" : rangePredicates,
                residualPredicates == null ? "" : residualPredicates
        );
    }
}
