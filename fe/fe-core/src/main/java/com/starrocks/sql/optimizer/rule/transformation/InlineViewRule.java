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

import com.starrocks.sql.optimizer.CardinalityPreservingJoinTablePruner;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLandingPadOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class InlineViewRule extends TransformationRule {
    public InlineViewRule() {
        super(RuleType.TF_INLINE_VIEW_RULE,
                Pattern.create(OperatorType.LOGICAL_LANDING_PAD, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLandingPadOperator landingPadOp = input.getOp().cast();
        ColumnRefSet columnRefSet = landingPadOp.getOutputColumns(new ExpressionContext(input));
        List<ColumnRefOperator> outputColRefs =
                columnRefSet.getStream().map(colRefId -> context.getColumnRefFactory().getColumnRef(colRefId))
                        .collect(Collectors.toList());
        CardinalityPreservingJoinTablePruner collector = new CardinalityPreservingJoinTablePruner();
        collector.collect(input);
        collector.pruneTables(outputColRefs);
        return collector.rewrite(input.inputAt(0))
                .map(Collections::singletonList)
                .orElse(Collections.singletonList(input.inputAt(0)));
    }
}
