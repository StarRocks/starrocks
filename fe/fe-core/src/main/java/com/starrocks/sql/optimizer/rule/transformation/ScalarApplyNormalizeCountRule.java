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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * The scalar aggregation in the subquery will be converted into a vector aggregation in scalar sub-query
 * but the scalar aggregation will return at least one row.
 * So we need to do special processing on count and the other count-like aggregate functions,
 * most other aggregate functions do not need special processing because they return NULL
 *
 * before:
 *          Apply(subquery operator = count)
 *         /      \
 *      Leaf       AGG(count)
 *
 * after:
 *          Apply(subquery operator = ifnull(count))
 *         /      \
 *      Leaf       AGG(count)
 *
 * A HAVING clause on the apply is moved into the subquery operator, see transform.
 */
public class ScalarApplyNormalizeCountRule extends TransformationRule {
    public ScalarApplyNormalizeCountRule() {
        super(RuleType.TF_SCALAR_APPLY_NORMALIZED_COUNT,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.LOGICAL_AGGR));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return apply.isScalar() && !SubqueryUtils.containsCorrelationSubquery(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = input.getOp().cast();
        LogicalAggregationOperator agg = input.inputAt(1).getOp().cast();

        ScalarOperator subqueryOperator = normalizeCount(apply.getSubqueryOperator(), agg.getAggregations());
        if (subqueryOperator == apply.getSubqueryOperator()) {
            return Collections.emptyList();
        }

        ScalarOperator predicate = apply.getPredicate();
        if (predicate != null && !apply.isNeedCheckMaxRows()
                && new ColumnRefSet(agg.getAggregations().keySet()).containsAll(predicate.getUsedColumns())) {
            // The predicate is the HAVING clause of the scalar aggregation. Evaluated in the outer join it would drop
            // the row and the subquery would return the normalized count instead of NULL, so evaluate it on the
            // normalized aggregations instead:
            //   CASE WHEN having(ifnull(count, 0)) THEN ifnull(count, 0) ELSE NULL END
            subqueryOperator = new CaseWhenOperator(subqueryOperator.getType(), null,
                    ConstantOperator.createNull(subqueryOperator.getType()),
                    Lists.newArrayList(normalizeCount(predicate, agg.getAggregations()), subqueryOperator));
            predicate = null;
        }

        LogicalApplyOperator newApply = LogicalApplyOperator.builder().withOperator(apply)
                .setSubqueryOperator(subqueryOperator)
                .setPredicate(predicate)
                .build();
        return Lists.newArrayList(OptExpression.create(newApply, input.getInputs()));
    }

    // Count-like aggregations that return 0, never NULL, on an empty input.
    private static final Set<String> ZERO_ON_EMPTY_FUNCTIONS = ImmutableSet.of(
            FunctionSet.COUNT,
            FunctionSet.COUNT_IF,
            FunctionSet.MULTI_DISTINCT_COUNT,
            FunctionSet.NDV,
            FunctionSet.APPROX_COUNT_DISTINCT,
            FunctionSet.BITMAP_UNION_COUNT,
            FunctionSet.BITMAP_UNION_INT,
            FunctionSet.HLL_UNION_AGG,
            FunctionSet.DS_HLL_COUNT_DISTINCT,
            FunctionSet.DS_THETA_COUNT_DISTINCT);

    public static boolean isZeroOnEmpty(CallOperator aggregation) {
        return ZERO_ON_EMPTY_FUNCTIONS.contains(aggregation.getFnName()) && aggregation.getType().isBigint();
    }

    // Whether the aggregation may return a non-NULL value on an empty input, e.g. 0 for count, an empty bitmap for
    // bitmap_union, or the result of an agg-state combinator such as count_merge.
    public static boolean isNonNullOnEmpty(CallOperator aggregation) {
        Function fn = aggregation.getFunction();
        return isZeroOnEmpty(aggregation)
                || FunctionSet.alwaysReturnNonNullableFunctions.contains(aggregation.getFnName())
                || (fn instanceof AggregateFunction && ((AggregateFunction) fn).returnsNonNullOnEmpty())
                || (fn != null && !fn.isNullable())
                || AggStateUtils.isAggStateCombinator(fn)
                || AggStateUtils.isAggStateCombinator(aggregation.getFnName());
    }

    // Replaces every count-like aggregation used by the subquery operator with ifnull(count, 0), see
    // isZeroOnEmpty. Returns the subquery operator itself if it uses none.
    public static ScalarOperator normalizeCount(ScalarOperator subqueryOperator,
                                                Map<ColumnRefOperator, CallOperator> aggregations) {
        List<ColumnRefOperator> countRefs = Lists.newArrayList();
        aggregations.forEach((k, v) -> {
            if (isZeroOnEmpty(v)) {
                countRefs.add(k);
            }
        });

        if (!subqueryOperator.getUsedColumns().containsAny(countRefs)) {
            return subqueryOperator;
        }

        Map<ColumnRefOperator, ScalarOperator> replaceMaps = Maps.newHashMap();
        for (ColumnRefOperator countRef : countRefs) {
            CallOperator call = new CallOperator(FunctionSet.IFNULL, IntegerType.BIGINT,
                    Lists.newArrayList(countRef, ConstantOperator.createBigint(0)),
                    ExprUtils.getBuiltinFunction(FunctionSet.IFNULL, new Type[] {IntegerType.BIGINT, IntegerType.BIGINT},
                            Function.CompareMode.IS_IDENTICAL));
            replaceMaps.put(countRef, call);
        }

        return new ReplaceColumnRefRewriter(replaceMaps).rewrite(subqueryOperator);
    }
}
