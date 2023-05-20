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
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PruneTableFunctionColumnRule extends TransformationRule {
    public PruneTableFunctionColumnRule() {
        super(RuleType.TF_PRUNE_TABLE_FUNCTION_COLUMNS,
                Pattern.create(OperatorType.LOGICAL_TABLE_FUNCTION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTableFunctionOperator logicalTableFunctionOperator = (LogicalTableFunctionOperator) input.getOp();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();
        List<ColumnRefOperator> newOuterCols = Lists.newArrayList();

        for (ColumnRefOperator col : logicalTableFunctionOperator.getOuterColRefs()) {
            if (requiredOutputColumns.contains(col.getId())) {
                newOuterCols.add(col);
            }
        }

        // Check whether we need to prune table function args
        List<ColumnRefOperator> fnResultColRefs = logicalTableFunctionOperator.getFnResultColRefs();
        List<Integer> prunedIndexes = Lists.newArrayList();
        for (int i = 0; i < fnResultColRefs.size(); ++i) {
            if (requiredOutputColumns.contains(fnResultColRefs.get(i))) {
                prunedIndexes.add(i);
            }
        }

        LogicalTableFunctionOperator newOperator;
        // prune table function args
        if (prunedIndexes.size() < fnResultColRefs.size()) {
            List<ColumnRefOperator> newFnResultColRefs = Lists.newArrayList();
            List<Pair<ColumnRefOperator, ScalarOperator>> newFnParamColumnProject = Lists.newArrayList();
            List<Type> tableFnReturnTypes = Lists.newArrayList();
            List<Type> tableFnArgTypes = Lists.newArrayList();
            TableFunction function = logicalTableFunctionOperator.getFn();
            for (Integer i : prunedIndexes) {
                newFnResultColRefs.add(fnResultColRefs.get(i));
                newFnParamColumnProject.add(logicalTableFunctionOperator.getFnParamColumnProject().get(i));

                tableFnReturnTypes.add(function.getTableFnReturnTypes().get(i));
                tableFnArgTypes.add(function.getArgs()[i]);
            }

            function = new TableFunction(function.getFunctionName(), function.getDefaultColumnNames(), tableFnArgTypes,
                    tableFnReturnTypes);

            newOperator = (new LogicalTableFunctionOperator.Builder())
                    .withOperator(logicalTableFunctionOperator)
                    .setOuterColRefs(newOuterCols)
                    .setFnResultColRefs(newFnResultColRefs)
                    .setFnParamColumnProject(newFnParamColumnProject)
                    .setFn(function)
                    .build();
        } else {
            newOperator = (new LogicalTableFunctionOperator.Builder())
                    .withOperator(logicalTableFunctionOperator)
                    .setOuterColRefs(newOuterCols)
                    .build();
        }

        for (Pair<ColumnRefOperator, ScalarOperator> pair : newOperator.getFnParamColumnProject()) {
            requiredOutputColumns.union(pair.first);
        }

        if (logicalTableFunctionOperator.equals(newOperator)) {
            return Collections.emptyList();
        }

        return Lists.newArrayList(OptExpression.create(newOperator, input.getInputs()));
    }
}