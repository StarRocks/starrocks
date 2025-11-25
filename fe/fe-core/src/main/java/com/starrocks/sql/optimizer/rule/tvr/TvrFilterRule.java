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
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptMeta;
import org.apache.hadoop.util.Lists;

import java.util.List;

public class TvrFilterRule extends TvrTransformationRule {

    public TvrFilterRule() {
        super(RuleType.TF_TVR_FILTER, Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return isSupportedTvr(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOperator = (LogicalFilterOperator) input.getOp();
        OptExpression child = input.inputAt(0);
        TvrOptMeta childTvrGroup = child.getTvrMeta();
        final TvrOptMeta tvrOptMeta = TvrOptMeta.mapChild(childTvrGroup, (childTvrVersionRange) -> {
            OptExpression result = OptExpression.create(filterOperator, childTvrVersionRange.optExpression());
            return new TvrOptExpression(childTvrVersionRange.tvrVersionRange(), result);
        });
        return Lists.newArrayList(OptExpression.create(filterOperator, tvrOptMeta, child));
    }
}
