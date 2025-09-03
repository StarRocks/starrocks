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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
 * The scalar aggregation in the subquery will be converted into a vector aggregation in scalar sub-query
 * but the scalar aggregation will return at least one row.
 * So we need to do special processing on count,
 * other aggregate functions do not need special processing because they return NULL
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

        List<ColumnRefOperator> countRefs = Lists.newArrayList();
        agg.getAggregations().forEach((k, v) -> {
            if (FunctionSet.COUNT.equals(v.getFnName())) {
                countRefs.add(k);
            }
        });

        if (!apply.getSubqueryOperator().getUsedColumns().containsAny(countRefs)) {
            return Collections.emptyList();
        }

        Map<ColumnRefOperator, ScalarOperator> replaceMaps = Maps.newHashMap();
        for (ColumnRefOperator countRef : countRefs) {
            CallOperator call = new CallOperator(FunctionSet.IFNULL, Type.BIGINT,
                    Lists.newArrayList(countRef, ConstantOperator.createBigint(0)),
                    Expr.getBuiltinFunction(FunctionSet.IFNULL, new Type[] {Type.BIGINT, Type.BIGINT},
                            Function.CompareMode.IS_IDENTICAL));
            replaceMaps.put(countRef, call);
        }

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(replaceMaps);
        LogicalApplyOperator newApply = LogicalApplyOperator.builder().withOperator(apply)
                .setSubqueryOperator(rewriter.rewrite(apply.getSubqueryOperator()))
                .build();
        return Lists.newArrayList(OptExpression.create(newApply, input.getInputs()));
    }
}
