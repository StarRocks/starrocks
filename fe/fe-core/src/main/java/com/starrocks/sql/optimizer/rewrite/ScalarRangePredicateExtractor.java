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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.property.RangeExtractor;
import com.starrocks.sql.optimizer.property.RangeExtractor.ValueDescriptor;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private ScalarOperator rewrite(ScalarOperator predicate, boolean onlyExtractColumnRef) {
        if (predicate == null || predicate.getOpType() != OperatorType.COMPOUND) {
            return predicate;
        }

        Set<ScalarOperator> conjuncts = Sets.newLinkedHashSet();
        conjuncts.addAll(Utils.extractConjuncts(predicate));
        predicate = Utils.compoundAnd(conjuncts);

        Map<ScalarOperator, ValueDescriptor> extractMap = extractImpl(predicate);

        Set<ScalarOperator> result = Sets.newLinkedHashSet();
        extractMap.keySet().stream().filter(k -> !onlyExtractColumnRef || k.isColumnRef())
                .map(extractMap::get)
                .filter(d -> d.getSourceCount() > 1)
                .map(ValueDescriptor::toScalarOperator).forEach(result::addAll);

        List<ScalarOperator> decimalKeys =
                extractMap.keySet().stream().filter(k -> !onlyExtractColumnRef || k.isColumnRef())
                        .filter(k -> k.getType().isDecimalOfAnyVersion()).collect(Collectors.toList());
        if (!decimalKeys.isEmpty()) {
            for (ScalarOperator key : decimalKeys) {
                ValueDescriptor vd = extractMap.get(key);
                vd.toScalarOperator().forEach(s -> Preconditions.checkState(
                        s.getChildren().stream().allMatch(c -> c.getType().matchesType(key.getType()))));
            }
        }

        ScalarOperator extractExpr = Utils.compoundAnd(Lists.newArrayList(result));
        if (extractExpr == null) {
            return predicate;
        }

        if (isOnlyOrCompound(predicate)) {
            Set<ColumnRefOperator> c = Sets.newHashSet(Utils.extractColumnRef(predicate));
            if (c.size() == extractMap.size() &&
                    extractMap.values().stream().allMatch(v -> v instanceof RangeExtractor.MultiValuesDescriptor)) {
                return extractExpr;
            }
        }

        if (isOnlyAndCompound(predicate)) {
            List<ScalarOperator> cs = Utils.extractConjuncts(predicate);
            Set<ColumnRefOperator> cf = new HashSet<>(Utils.extractColumnRef(predicate));

            // getSourceCount = cs.size() means that all and components have the same column ref
            // and it can be merged into one range predicate. mistakenly, when the predicate is
            // date_trunc(YEAR, dt) = '2024-01-01' AND mode = 'Buzz' AND
            // date_trunc(YEAR, dt) = '2024-01-01' AND mode = 'Buzz' //duplicate
            //
            // only mode = 'Buzz' is a extractable range predicate, so its corresponding ValueDescriptor's
            // sourceCount = 2(since it occurs twice), and cs(it is also 2 in this example)are number of
            // unique column refs of the predicate, the two values are properly equivalent, so it yields
            // wrong result.
            //
            // Components of AND/OR should be deduplicated at first to avoid this issue.
            if (extractMap.values().stream().allMatch(valueDescriptor -> valueDescriptor.getSourceCount() == cs.size())
                    && extractMap.size() == cf.size()) {
                if (result.size() == conjuncts.size()) {
                    // to keep the isPushdown/isRedundant of predicate
                    return predicate;
                } else {
                    return extractExpr;
                }
            }
        }

        if (!conjuncts.containsAll(result)) {
            // remove duplicates
            result.removeAll(conjuncts);
            extractExpr = Utils.compoundAnd(Lists.newArrayList(result));
            result.forEach(f -> f.setFromPredicateRangeDerive(true));
            result.stream().filter(predicateOperator -> !checkStatisticsEstimateValid(predicateOperator))
                    .forEach(f -> f.setNotEvalEstimate(true));
            // The newly extracted predicate will not be used to estimate the statistics,
            // which will cause the cardinality to be too small
            extractExpr.setFromPredicateRangeDerive(true);
            if (!checkStatisticsEstimateValid(extractExpr)) {
                extractExpr.setNotEvalEstimate(true);
            }
            // TODO: merge `setFromPredicateRangeDerive` into `setRedundant`
            result.forEach(f -> f.setRedundant(true));
            extractExpr.setRedundant(true);
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

    private Map<ScalarOperator, ValueDescriptor> extractImpl(ScalarOperator scalarOperator) {
        RangeExtractor re = new RangeExtractor();
        return re.apply(scalarOperator, null);
    }

    private static boolean isOnlyAndCompound(ScalarOperator predicate) {
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            if (!compoundPredicateOperator.isAnd()) {
                return false;
            }

            return isOnlyAndCompound(compoundPredicateOperator.getChild(0)) &&
                    isOnlyAndCompound(compoundPredicateOperator.getChild(1));
        } else {
            return true;
        }
    }

    private static boolean isOnlyOrCompound(ScalarOperator predicate) {
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            if (!compoundPredicateOperator.isOr()) {
                return false;
            }

            return isOnlyOrCompound(predicate.getChild(0)) && isOnlyOrCompound(predicate.getChild(1));
        } else {
            return true;
        }
    }
}