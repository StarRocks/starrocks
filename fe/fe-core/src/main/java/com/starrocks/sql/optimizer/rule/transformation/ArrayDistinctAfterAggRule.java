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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*      project(array_distinct)           project
 *                |               =>         |
 *      Aggregation(array_agg)            Aggregation(array_agg_distinct)
 *
 * project may have projection use result of array_agg in array_distinct or in an expr like
 * array_length(array_distinct(array_agg(a))), this rule check all the projection and predicate,
 *  if all of them use the array_agg's result with array_distinct that we can just do array_agg_distinct.
 */
public class ArrayDistinctAfterAggRule extends TransformationRule {
    public ArrayDistinctAfterAggRule() {
        super(RuleType.TF_ARRAY_DISTINCT_AFTER_AGG, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getInputs().get(0).getOp();
        return aggregate.getAggregations().values().stream().anyMatch(x -> x.getFnName().equals(FunctionSet.ARRAY_AGG));
    }

    private boolean checkScalarOp(ColumnRefOperator col, ScalarOperator op) {
        if (!op.getColumnRefs().contains(col)) {
            return true;
        }
        ScalarOperatorVisitor<Boolean, Void> visitor = new ScalarOperatorVisitor<Boolean, Void>() {
            @Override
            public Boolean visit(ScalarOperator scalarOperator, Void context) {
                for (ScalarOperator child : scalarOperator.getChildren()) {
                    if (child.getColumnRefs().contains(col)) {
                        boolean ret = child.accept(this, null);
                        if (!ret) {
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public Boolean visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                return !columnRefOperator.equals(col);
            }

            @Override
            public Boolean visitCall(CallOperator callOperator, Void context) {
                if (callOperator.getFnName().equals(FunctionSet.ARRAY_DISTINCT)) {
                    return callOperator.getArguments().size() == 1 && callOperator.getArguments().get(0).equals(col);
                } else {
                    return visit(callOperator, null);
                }
            }
        };

        return op.accept(visitor, null);
    }

    private boolean checkAllUseOfArrayAggResultHasDistinct(ColumnRefOperator colRef, LogicalProjectOperator project,
                                                           LogicalAggregationOperator agg) {
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : project.getColumnRefMap().entrySet()) {
            if (!checkScalarOp(colRef, entry.getValue())) {
                return false;
            }
        }
        if (agg.getPredicate() != null) {
            return checkScalarOp(colRef, agg.getPredicate());
        }
        return true;
    }

    private ScalarOperator rewriteScalarOp(ColumnRefOperator oldCol, ColumnRefOperator newCol, ScalarOperator op) {
        if (!op.getColumnRefs().contains(oldCol)) {
            return op;
        }

        ScalarOperatorVisitor<ScalarOperator, Void> visitor = new ScalarOperatorVisitor<ScalarOperator, Void>() {
            @Override
            public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
                for (int i = 0; i < children.size(); ++i) {
                    ScalarOperator child = children.get(i);
                    if (child.getColumnRefs().contains(oldCol)) {
                        scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
                    }
                }
                return scalarOperator;
            }

            @Override
            public ScalarOperator visitCall(CallOperator call, Void context) {
                if (call.getFnName().equals(FunctionSet.ARRAY_DISTINCT)
                        && call.getArguments().size() == 1
                        && call.getArguments().get(0).equals(oldCol)) {
                    return newCol;
                }
                return visit(call, null);
            }
        };

        return op.accept(visitor, null);
    }

    private void rewriteProject(ColumnRefOperator oldCol, ColumnRefOperator newCol,
                                            LogicalProjectOperator project) {
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : project.getColumnRefMap().entrySet()) {
            newColumnRefMap.put(entry.getKey(), rewriteScalarOp(oldCol, newCol, entry.getValue()));
        }
        project.getColumnRefMap().clear();
        newColumnRefMap.forEach((k, v) -> project.getColumnRefMap().put(k, v));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator project = (LogicalProjectOperator) input.getOp();
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getInputs().get(0).getOp();

        ScalarOperator newPredicate = aggregate.getPredicate();

        Map<ColumnRefOperator, CallOperator> replaceMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregate.getAggregations().entrySet()) {
            if (entry.getValue().getFnName().equals(FunctionSet.ARRAY_AGG) &&
                    checkAllUseOfArrayAggResultHasDistinct(entry.getKey(), project, aggregate)) {
                Function oldFn = entry.getValue().getFunction();
                Function newFn = Expr.getBuiltinFunction(FunctionSet.ARRAY_AGG_DISTINCT, oldFn.getArgs(),
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                if (newFn == null) {
                    // array_agg_distinct not support this args.
                    replaceMap.put(entry.getKey(), entry.getValue());
                    continue;
                }
                if (oldFn.getArgs()[0].isDecimalOfAnyVersion()) {
                    newFn = DecimalV3FunctionAnalyzer.rectifyAggregationFunction(
                            (AggregateFunction) newFn, oldFn.getArgs()[0], oldFn.getReturnType());
                }
                CallOperator newCall = new CallOperator(newFn.getFunctionName().getFunction(), newFn.getReturnType(),
                        entry.getValue().getArguments(), newFn);
                ColumnRefOperator oldCol = entry.getKey();
                ColumnRefOperator newCol = new ColumnRefOperator(
                        oldCol.getId(), oldCol.getType(), newFn.functionName(), oldCol.isNullable());
                replaceMap.put(newCol, newCall);
                rewriteProject(oldCol, newCol, project);
                if (newPredicate != null) {
                    rewriteScalarOp(oldCol, newCol, newPredicate);
                }
            } else {
                replaceMap.put(entry.getKey(), entry.getValue());
            }
        }

        LogicalAggregationOperator newAggOp = LogicalAggregationOperator.builder().withOperator(aggregate)
                .setAggregations(replaceMap).setPredicate(newPredicate).build();
        return Lists.newArrayList(OptExpression.create(project,
                OptExpression.create(newAggOp, input.getInputs().get(0).getInputs().get(0))));
    }
}
