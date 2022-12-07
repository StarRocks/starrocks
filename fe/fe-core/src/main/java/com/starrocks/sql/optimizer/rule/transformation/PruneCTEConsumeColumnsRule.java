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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class PruneCTEConsumeColumnsRule extends TransformationRule {
    public PruneCTEConsumeColumnsRule() {
        super(RuleType.TF_PRUNE_CTE_CONSUME_COLUMNS, Pattern.create(OperatorType.LOGICAL_CTE_CONSUME));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) input.getOp();

        ColumnRefSet requiredConsumeOutput = context.getTaskContext().getRequiredColumns();

        // predicate
        if (null != consume.getPredicate()) {
            requiredConsumeOutput.union(consume.getPredicate().getUsedColumns());
        }

        // required
        Map<ColumnRefOperator, ColumnRefOperator> mapping = Maps.newHashMap();
        consume.getCteOutputColumnRefMap().forEach((k, v) -> {
            if (requiredConsumeOutput.contains(k)) {
                mapping.put(k, v);
                requiredConsumeOutput.union(v);
            }
        });

        if (mapping.isEmpty()) {
            ColumnRefOperator key =
                    Utils.findSmallestColumnRef(Lists.newArrayList(consume.getCteOutputColumnRefMap().keySet()));
            mapping.put(key, consume.getCteOutputColumnRefMap().get(key));
            requiredConsumeOutput.union(key);
            requiredConsumeOutput.union(consume.getCteOutputColumnRefMap().get(key));
        }

        LogicalCTEConsumeOperator c =
                new LogicalCTEConsumeOperator.Builder().withOperator(consume).setCteOutputColumnRefMap(mapping).build();
        return Lists.newArrayList(OptExpression.create(c, input.getInputs()));
    }
}
