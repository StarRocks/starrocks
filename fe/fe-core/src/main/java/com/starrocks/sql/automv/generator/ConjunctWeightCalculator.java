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

package com.starrocks.sql.automv.generator;

import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.StrictOp;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

// A MV's dimension columns can be used for two purposes:
// 1. the dimension columns come from conjuncts and are used to filter data;
// 2. the dimension columns come from group-by clauses.
//
// There are zone-map index and short-key index on sort key of the Table, so
// we should put conjuncts' columns in sort key. however, instead of conjuncts of
// all kinds, only simple conjuncts can use zone-map index and short-key index to
// filter rows, the simple conjuncts are such as:
// 1. eq conjuncts;
// 2. range conjuncts;
// 3. is null/is not null conjuncts;
// 4. in conjuncts;
// 5. others.
//
// So we should place columns used by these conjuncts in sort key.
// short key index has a pre-defined length(36bytes), it can hold several fixed-length
// columns but only one variable-length column that should be arranged at the tail
// of the short-key index and may be truncated during builtin short-key index. so
// sort key selection policy should consider:
// 1. eq/range/in/is null/is not null conjuncts' column should be chosen;
// 2. fixed-length column should take precedence over variable-length column, and the former
// should be put in front of sort-key while the latter should be put in the tail.
//
// The conjuncts' columns has more chance to use index, the larger weight would be
// assigned to the conjuncts. for examples: eq conjuncts' weight is
// MAX_WEIGHT/SCORE_EQ = 100.0/1.0; like conjuncts' weight is is MAX_WEIGHT = 100.0/7.0;
public class ConjunctWeightCalculator {
    private static final double MAX_WEIGHT = 100.0;
    private static final double SCORE_EQ = 1.0;
    private static final double SCORE_IS_NULL = 2.0;
    private static final double SCORE_IS_NOT_NULL = 2.0;
    private static final double SCORE_IN = 2.0;
    //private static final double SCORE_BILATERAL_RANGE = 3.0;
    private static final double SCORE_UNILATERAL_RANGE = 4.0;
    private static final double SCORE_NOT_IN = 5.0;
    private static final double SCORE_NOT_EQ = 6.0;
    private static final double SCORE_LIKE = 7.0;
    private static final double SCORE_NOT_LIKE = 8.0;

    private static final double SCORE_UNKNOWN = 20.0;

    public static Function<Pair<Integer, GenericColumn>, Double> getColumnWeightCalculatorForAggMV(
            List<Op> hoistConjuncts, List<Op> nonHoistConjuncts, ColumnRefSet dimensionIds) {

        ConjunctWeightCalculator weightCalculator = new ConjunctWeightCalculator();
        Map<Integer, Double> weights1 = weightCalculator.calculate(hoistConjuncts);
        Map<Integer, Double> weights2 = weightCalculator.calculate(nonHoistConjuncts);
        return p -> {
            double g = dimensionIds.contains(p.first) && p.second.getType().canDistributedBy() ? 1 : 0;
            double h = weights1.getOrDefault(p.first, 0.0) * 1.5 + weights2.getOrDefault(p.first, 0.0);
            int w = p.second.getType().getPrimitiveType().isVariableLengthType() ? 1 : 10;
            return -g * (h + 10 * w);
        };
    }

    public static Function<Pair<Integer, GenericColumn>, Double> getColumnWeightCalculatorFor11MV(
            Map<Integer, Map<StrictOp, Long>> conjunctFreq) {

        ConjunctWeightCalculator weightCalculator = new ConjunctWeightCalculator();
        Map<Integer, Double> weights = weightCalculator.calculate(conjunctFreq);
        ColumnRefSet ids = ColumnRefSet.createByIds(weights.keySet());
        return p -> {
            double g = ids.contains(p.first) && p.second.getType().canDistributedBy() ? 1 : 0;
            double h = weights.getOrDefault(p.first, 0.0);
            int w = p.second.getType().getPrimitiveType().isVariableLengthType() ? 1 : 10;
            return -g * (h + 10 * w);
        };
    }

    double getConjunctScore(Op conjunct) {
        if ((conjunct.isEq() || conjunct.isNullSafeEq())) {
            return SCORE_EQ;
        } else if (conjunct.isVarIsNull()) {
            return SCORE_IS_NULL;
        } else if (conjunct.isVarIsNotNull()) {
            return SCORE_IS_NOT_NULL;
        } else if (conjunct.isIn()) {
            return SCORE_IN;
        } else if (conjunct.isInRange()) {
            return SCORE_UNILATERAL_RANGE;
        } else if (conjunct.isNotIn()) {
            return SCORE_NOT_IN;
        } else if (conjunct.isNe()) {
            return SCORE_NOT_EQ;
        } else if ((conjunct.isLike() || conjunct.isRegexp())) {
            return SCORE_LIKE;
        } else if ((conjunct.isNotLike() || conjunct.isNotRegex()) && conjunct.unmodified().arg(0).isVar()) {
            return SCORE_NOT_LIKE;
        } else {
            return SCORE_UNKNOWN;
        }
    }

    private Optional<Pair<Integer, Double>> calculate(Op conjunct) {
        Op subject = conjunct.unmodified();
        subject = subject.isApply() ? conjunct.unmodified().arg(0) : subject;

        if (subject.isVar()) {
            int id = subject.getId();
            return Optional.of(Pair.create(id, MAX_WEIGHT / getConjunctScore(conjunct)));
        } else if (subject.getIdSet().size() == 1) {
            int id = subject.getIdSet().iterator().next();
            if (subject.isApply() && subject.getArgs().stream().anyMatch(arg -> arg.isVar() && arg.getId() == id)) {
                return Optional.of(Pair.create(id, MAX_WEIGHT / 2.0 / getConjunctScore(conjunct)));
            } else {
                return Optional.of(Pair.create(id, MAX_WEIGHT / 4.0 / getConjunctScore(conjunct)));
            }
        } else {
            return Optional.empty();
        }
    }

    public Map<Integer, Double> calculate(List<Op> conjuncts) {
        Map<Integer, Double> columnWeights = Maps.newHashMap();
        conjuncts.stream()
                .map(this::calculate)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(p -> columnWeights.merge(p.first, p.second, Double::sum));
        return columnWeights;
    }

    public Map<Integer, Double> calculate(Map<Integer, Map<StrictOp, Long>> conjunctFreq) {
        Map<Integer, Double> columnWeights = Maps.newHashMap();
        conjunctFreq.entrySet()
                .stream()
                .flatMap(e -> e.getValue().entrySet()
                        .stream()
                        .map(ee -> Pair.create(ee.getKey().getOp(), Pair.create(e.getKey(), ee.getValue()))))
                .map(p -> Pair.create(this.calculate(p.first).map(pp -> pp.second).orElse(0.0), p.second))
                .map(p -> Pair.create(p.second.first, p.first * p.second.second))
                .forEach(p -> columnWeights.merge(p.first, p.second, Double::sum));
        return columnWeights;
    }
}
