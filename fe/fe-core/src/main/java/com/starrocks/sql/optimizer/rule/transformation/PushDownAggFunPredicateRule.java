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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/*
case1:
select v1, min(v2) from t group by v1 having min(v2) < 2
                             |
                            \|/
select prd_t.v1, min(prd_t.v2) from (select v1, v2 from t where v2 < 2) prd_t group by prd_t.v1 having min(prd_t.v2) < 2
case2:
select v1, max(v2) from t group by v1 having max(v2) >= 2
                             |
                            \|/
select prd_t.v1, max(prd_t.v2) from (select v1, v2 from t where v2 >= 2) prd_t group by prd_t.v1 having max(prd_t.v2) >= 2
 */
public class PushDownAggFunPredicateRule extends TransformationRule {

    public PushDownAggFunPredicateRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_FUN_PREDICATE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator logicalAggOperator = (LogicalAggregationOperator) input.getOp();
        if (logicalAggOperator.getAggregations().size() != 1 || logicalAggOperator.getPredicate() == null) {
            return false;
        }

        Optional<BinaryPredicateOperator> matchedPredicateOpt = getMatchedPredicate(logicalAggOperator);
        if (matchedPredicateOpt.isEmpty()) {
            return false;
        }
        BinaryPredicateOperator matchedPredicate = matchedPredicateOpt.get();
        if (isSetPushDownAggFunPrdTag(matchedPredicate)) {
            return false;
        }

        CallOperator aggFun = logicalAggOperator.getAggregations().get(matchedPredicate.getChild(0));
        BinaryType matchedPredOptType = matchedPredicate.getBinaryType();
        return (aggFun.getFnName().equalsIgnoreCase("min") && (matchedPredOptType.equals(BinaryType.EQ) ||
                matchedPredOptType.equals(BinaryType.LE) || matchedPredOptType.equals(BinaryType.LT))) ||
                (aggFun.getFnName().equalsIgnoreCase("max") && (matchedPredOptType.equals(BinaryType.EQ) ||
                        matchedPredOptType.equals(BinaryType.GE) || matchedPredOptType.equals(BinaryType.GT)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregateOperator = (LogicalAggregationOperator) input.getOp();
        ScalarOperator pushDownPrd = buildScalarPrdFormAggFunPrd(aggregateOperator);
        OptExpression pushDownFilter = new OptExpression(new LogicalFilterOperator(pushDownPrd));
        pushDownFilter.getInputs().addAll(input.getInputs());
        input.getInputs().clear();
        input.getInputs().add(pushDownFilter);
        return Lists.newArrayList(input);
    }

    private ScalarOperator buildScalarPrdFormAggFunPrd(LogicalAggregationOperator logicalAggOperator) {
        BinaryPredicateOperator matchedPredicate = getMatchedPredicate(logicalAggOperator).get();
        CallOperator aggFun = logicalAggOperator.getAggregations().get(matchedPredicate.getChild(0));

        ScalarOperator resultPredicate;
        if (aggFun.getFnName().equalsIgnoreCase("min") && matchedPredicate.getBinaryType().equals(BinaryType.EQ)) {
            resultPredicate = new BinaryPredicateOperator(BinaryType.LE, aggFun.getChildren().get(0),
                    matchedPredicate.getChild(1));
        } else if (aggFun.getFnName().equalsIgnoreCase("max") &&
                matchedPredicate.getBinaryType().equals(BinaryType.EQ)) {
            resultPredicate = new BinaryPredicateOperator(BinaryType.GE, aggFun.getChildren().get(0),
                    matchedPredicate.getChild(1));
        } else {
            resultPredicate = new BinaryPredicateOperator(matchedPredicate.getBinaryType(),
                    aggFun.getChildren().get(0), matchedPredicate.getChild(1));
        }
        setPushDownAggFunPrdTag(matchedPredicate);
        return resultPredicate;
    }

    private Optional<BinaryPredicateOperator> getMatchedPredicate(LogicalAggregationOperator logicalAggOperator) {
        List<ScalarOperator> sourcePrd = Utils.extractConjuncts(logicalAggOperator.getPredicate());
        Map<ColumnRefOperator, CallOperator> aggFunMap = logicalAggOperator.getAggregations();
        for (ScalarOperator so : sourcePrd) {
            if (so instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binarySo = (BinaryPredicateOperator) so;
                ScalarOperator key = binarySo.getChild(0);
                CallOperator aggFun = aggFunMap.get(key);
                if (aggFun != null) {
                    return Optional.of(binarySo);
                }
            }
        }
        return Optional.empty();
    }

    private void setPushDownAggFunPrdTag(BinaryPredicateOperator matchedPredicate) {
        List newHints = Lists.newArrayList(matchedPredicate.getHints());
        newHints.add("PushDownAggFunPrdHint");
        matchedPredicate.setHints(newHints);
    }

    private boolean isSetPushDownAggFunPrdTag(BinaryPredicateOperator matchedPredicate) {
        return matchedPredicate.getHints().contains("PushDownAggFunPrdHint");
    }
}
