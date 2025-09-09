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
// limitations under the License

package com.starrocks.sql.optimizer.rule.tvr;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrLazyOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptMeta;
import org.apache.hadoop.util.Lists;

import java.util.List;
import java.util.stream.Collectors;

public class TvrUnionAllRule extends TvrTransformationRule {

    public TvrUnionAllRule() {
        super(RuleType.TF_TVR_UNION_ALL,
                Pattern.create(OperatorType.LOGICAL_UNION, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!isSupportedTvr(input)) {
            return false;
        }
        LogicalUnionOperator logicalUnionOperator = input.getOp().cast();
        return logicalUnionOperator.isUnionAll();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalUnionOperator logicalUnionOperator = input.getOp().cast();
        List<ColumnRefOperator> originalOutputColRefs = logicalUnionOperator.getOutputColumnRefOp();
        TvrOptMeta childTvrMeta = input.getInputs().get(0).getTvrMeta();
        TvrLazyOptExpression fromTvrOptExpression = TvrLazyOptExpression.of(() -> {
            List<TvrLazyOptExpression> fromChildrenTvrOptExpressions = input.getInputs()
                    .stream()
                    .map(optExpression -> optExpression.getTvrMeta().from())
                    .collect(Collectors.toList());
            List<OptExpressionWithOutput> fromChildren = fromChildrenTvrOptExpressions
                    .stream()
                    .map(lazy -> lazy.get())
                    .map(opt -> duplicateOptExpression(context, opt.optExpression(), originalOutputColRefs))
                    .collect(Collectors.toList());
            OptExpression fromOptExpression = buildUnionOperator(childTvrMeta, originalOutputColRefs, fromChildren);
            return new TvrOptExpression(childTvrMeta.getFrom().tvrVersionRange(), fromOptExpression);
        });
        TvrLazyOptExpression toTvrOptExpression = TvrLazyOptExpression.of(() -> {
            List<TvrLazyOptExpression> toChildrenTvrOptExpressions = input.getInputs()
                    .stream()
                    .map(optExpression -> optExpression.getTvrMeta().to())
                    .collect(Collectors.toList());
            List<OptExpressionWithOutput> toChildren = toChildrenTvrOptExpressions
                    .stream()
                    .map(lazy -> lazy.get())
                    .map(opt -> duplicateOptExpression(context, opt.optExpression(), originalOutputColRefs))
                    .collect(Collectors.toList());
            OptExpression toOptExpression = buildUnionOperator(childTvrMeta, originalOutputColRefs, toChildren);
            return new TvrOptExpression(childTvrMeta.getFrom().tvrVersionRange(), toOptExpression);
        });
        TvrOptMeta tvrOptMeta = new TvrOptMeta(childTvrMeta.tvrDeltaTrait(), fromTvrOptExpression, toTvrOptExpression);
        OptExpression result = OptExpression.create(logicalUnionOperator, tvrOptMeta, input.getInputs());
        return Lists.newArrayList(result);
    }
}
