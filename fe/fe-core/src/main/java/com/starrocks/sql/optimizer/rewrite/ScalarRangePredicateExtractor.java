// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;
import java.util.Set;

/**
 * Derive a expression's value range. such as:
 * select * from t0 where v1 > 1 AND v1 < 5 => v1 is (1, 5)
 * select * from t0 where (v1 > 1 AND v1 < 5) OR (v1 = 3 OR v1 = 6) => v1 is (1, 6]
 * select * from t0 where (v1 + 1) = 3 AND (v1 + 1) = 6 => (v1 + 1) must be null
 * <p>
 * The core of the algorithm:
 * 1. Take the common set of the predicates on children of or, and compute UNION set on the same expressions
 * 2. Take the full set of the predicates on children of AND, and compute INTERSECT set on the same expressions
 */
public class ScalarRangePredicateExtractor {
    public ScalarOperator rewriteOnlyColumn(ScalarOperator predicate) {
        return rewrite(predicate, true);
    }

    public ScalarOperator rewriteAll(ScalarOperator predicate) {
        return rewrite(predicate, false);
    }

    public ScalarOperator rewrite(ScalarOperator predicate, boolean onlyExtractColumnRef) {
        if (predicate.getOpType() != OperatorType.COMPOUND) {
            return predicate;
        }

        Set<ScalarOperator> conjuncts = Sets.newLinkedHashSet();
        conjuncts.addAll(Utils.extractConjuncts(predicate));

        Map<ScalarOperator, RangeExtractor.ValueDescriptor> hashMap = extractImpl(predicate);

        Set<ScalarOperator> result = Sets.newLinkedHashSet();
        hashMap.keySet().stream().filter(k -> !onlyExtractColumnRef || k.isColumnRef())
                .map(hashMap::get)
                .filter(d -> d.sourceCount > 1)
                .map(RangeExtractor.ValueDescriptor::toScalarOperator).forEach(result::addAll);
        result.removeIf(conjuncts::contains);
        result.forEach(f -> f.setFromPredicateRangeDerive(true));
        result.stream().filter(predicateOperator -> !checkStatisticsEstimateValid(predicateOperator))
                .forEach(f -> f.setNotEvalEstimate(true));

        ScalarOperator extractExpr = Utils.compoundAnd(Lists.newArrayList(result));
        predicate = Utils.compoundAnd(Lists.newArrayList(conjuncts));
        if (extractExpr != null && !conjuncts.contains(extractExpr)) {
            // The newly extracted predicate will not be used to estimate the statistics,
            // which will cause the cardinality to be too small
            extractExpr.setFromPredicateRangeDerive(true);
            if (!checkStatisticsEstimateValid(extractExpr)) {
                extractExpr.setNotEvalEstimate(true);
            }
            return Utils.compoundAnd(predicate, extractExpr);
        }

        return predicate;
    }

    private boolean checkStatisticsEstimateValid(ScalarOperator predicate) {
        for (ScalarOperator child : predicate.getChildren()) {
            if (!checkStatisticsEstimateValid(child)) {
                return false;
            }
        }
        // we can not estimate the char/varchar type Min/Max column statistics
        if (predicate.getType().isStringType()) {
            return false;
        }
        return true;
    }

    private Map<ScalarOperator, RangeExtractor.ValueDescriptor> extractImpl(ScalarOperator scalarOperator) {
        RangeExtractor re = new RangeExtractor();
        return re.apply(scalarOperator, null);
    }
}