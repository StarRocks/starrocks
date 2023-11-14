// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

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

    // split predicate into three parts: equal columns predicates, range predicates, and residual predicates
    public static PredicateSplit splitPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return PredicateSplit.of(null, null, null);
        }
        PredicateExtractor extractor = new PredicateExtractor();
        RangePredicate rangePredicate =
                predicate.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
        ScalarOperator equalityConjunct = Utils.compoundAnd(extractor.getColumnEqualityPredicates());
        ScalarOperator rangeConjunct = null;
        ScalarOperator residualConjunct = Utils.compoundAnd(extractor.getResidualPredicates());
        if (rangePredicate != null) {
            // convert rangePredicate to rangeConjunct
            rangeConjunct = rangePredicate.toScalarOperator();
        } else if (extractor.getColumnEqualityPredicates().isEmpty() && extractor.getResidualPredicates().isEmpty()) {
            residualConjunct = Utils.compoundAnd(residualConjunct, predicate);
        }
        return PredicateSplit.of(equalityConjunct, rangeConjunct, residualConjunct);
    }
}
