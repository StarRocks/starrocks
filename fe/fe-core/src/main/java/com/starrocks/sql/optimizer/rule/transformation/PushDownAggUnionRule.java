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
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.AggType.LOCAL;

// Before:
//      Global Aggregate
//            |
//      Local Aggregate
//            |
//          Union
//         /     \
//      Leaf     Leaf
// After:
//      Global Aggregate
//             |
//           Union
//        /         \
//   Local Agg    Local Agg
//       |            |
//     Leaf          Leaf


public class PushDownAggUnionRule extends TransformationRule {

    private static final PushDownAggUnionRule INSTANCE = new PushDownAggUnionRule();

    public static PushDownAggUnionRule getInstance() {
        return INSTANCE;
    }

    public PushDownAggUnionRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_UNION, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)
                                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnablePushDownAggUnion()) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalUnionOperator union = (LogicalUnionOperator) input.inputAt(0).getOp();
        return agg.getType() == LOCAL && union.isUnionAll();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator originAgg = (LogicalAggregationOperator) input.getOp();
        LogicalUnionOperator union = (LogicalUnionOperator) input.inputAt(0).getOp();
        List<OptExpression> newAggs = new ArrayList<>();
        List<List<ColumnRefOperator>> unionInputs = new ArrayList<>();
        List<OptExpression> leaves = new ArrayList<>();

        // Refactor leaf nodes, push down the projection of union operator to the leaves.
        if (union.getProjection() != null) {
            for (int i = 0; i < union.getChildOutputColumns().size(); i++) {
                final int finalIndex = i;
                Operator child = input.inputAt(0).inputAt(i).getOp();
                ScalarOperatorVisitor<ScalarOperator, Void> valueVisitor = new ScalarOperatorVisitor<ScalarOperator, Void>() {
                    @Override
                    public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                        List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
                        for (int i = 0; i < children.size(); ++i) {
                            scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
                        }
                        return scalarOperator;
                    }

                    @Override
                    public ScalarOperator visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                        int index = union.getOutputColumnRefOp().indexOf(columnRefOperator);
                        if (index == -1) {
                            return columnRefOperator;
                        }
                        ColumnRefOperator childInput = union.getChildOutputColumns().get(finalIndex).get(index);
                        if (child.getProjection() == null) {
                            return childInput;
                        }
                        ScalarOperator scanProject = child.getProjection().getColumnRefMap().get(childInput);
                        if (scanProject == null) {
                            ScalarOperator commonSubOperator = child.getProjection().getCommonSubOperatorMap().get(childInput);
                            if (commonSubOperator != null) {
                                return commonSubOperator;
                            }
                            return columnRefOperator;
                        }
                        return scanProject;
                    }
                };

                ScalarOperatorVisitor<ScalarOperator, Void> keyVisitor = new ScalarOperatorVisitor<ScalarOperator, Void>() {
                    @Override
                    public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                        List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
                        for (int i = 0; i < children.size(); ++i) {
                            scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
                        }
                        return scalarOperator;
                    }

                    @Override
                    public ScalarOperator visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                        int index = union.getOutputColumnRefOp().indexOf(columnRefOperator);
                        if (index == -1) {
                            return columnRefOperator;
                        }
                        return union.getChildOutputColumns().get(finalIndex).get(index);
                    }
                };

                // The visitor might change the projection, so clone it first.
                Projection tempProject = union.getProjection().deepClone();
                Map<ColumnRefOperator, ScalarOperator> newColumnRefMap =
                        tempProject.getColumnRefMap().entrySet().stream().collect(Collectors.toMap(
                                entry -> (ColumnRefOperator) entry.getKey().accept(keyVisitor, null),
                                entry -> entry.getValue().accept(valueVisitor, null)));
                Map<ColumnRefOperator, ScalarOperator> newCommonSubOperatorMap =
                        tempProject.getCommonSubOperatorMap().entrySet().stream().collect(Collectors.toMap(
                                entry -> (ColumnRefOperator) entry.getKey().accept(keyVisitor, null),
                                entry -> entry.getValue().accept(valueVisitor, null)));
                Projection newProject = new Projection(newColumnRefMap, newCommonSubOperatorMap,
                        tempProject.needReuseLambdaDependentExpr());

                LogicalOperator.Builder builder = OperatorBuilderFactory.build(child);
                builder.withOperator(child);
                builder.setProjection(newProject);
                Operator clonedChild = builder.build();
                clonedChild.getRowOutputInfo(new ArrayList<>());
                leaves.add(OptExpression.create(clonedChild, input.inputAt(0).inputAt(i).getInputs()));
            }
        }

        // Modify the inputs and outputs of union operator and local aggregation operators.
        for (int i = 0; i < union.getChildOutputColumns().size(); i++) {
            final int finalI = i;
            ScalarOperatorVisitor<ScalarOperator, Void> visitor = new ScalarOperatorVisitor<ScalarOperator, Void>() {
                @Override
                public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                    List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
                    for (int i = 0; i < children.size(); ++i) {
                        scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
                    }
                    return scalarOperator;
                }

                @Override
                public ScalarOperator visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                    int index = union.getOutputColumnRefOp().indexOf(columnRefOperator);
                    if (index == -1) {
                        return union.getRowOutputInfo(null).getColOutputInfo().get(columnRefOperator.getId()).getColumnRef();
                    }
                    return union.getChildOutputColumns().get(finalI).get(index);
                }
            };
            Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : originAgg.getAggregations().entrySet()) {
                CallOperator newFn = (CallOperator) entry.getValue().clone();
                for (int index = 0; index < entry.getValue().getArguments().size(); index++) {
                    ScalarOperator argument = newFn.getArguments().get(index);
                    if (argument.isConstantRef()) {
                        newFn.setChild(index, argument);
                    } else {
                        newFn.setChild(index, argument.accept(visitor, null));
                    }
                }
                newAggMap.put(entry.getKey(), newFn);
            }
            List<ColumnRefOperator> newGroupByKeys = new ArrayList<>(originAgg.getGroupingKeys().size());
            for (ColumnRefOperator groupByKey : originAgg.getGroupingKeys()) {
                newGroupByKeys.add((ColumnRefOperator) groupByKey.accept(visitor, null));
            }
            List<ColumnRefOperator> newPartitionKeys = new ArrayList<>(originAgg.getPartitionByColumns().size());
            for (ColumnRefOperator partitionKey : originAgg.getPartitionByColumns()) {
                newPartitionKeys.add((ColumnRefOperator) partitionKey.accept(visitor, null));
            }
            LogicalAggregationOperator newAgg = new LogicalAggregationOperator.Builder().withOperator(originAgg)
                    .setGroupingKeys(newGroupByKeys)
                    .setPartitionByColumns(newPartitionKeys)
                    .setAggregations(createNormalAgg(newAggMap))
                    .setPredicate(null)
                    .setLimit(Operator.DEFAULT_LIMIT)
                    .setProjection(null)
                    .build();

            OptExpression newAggExpr;
            if (leaves.size() > 0) {
                newAggExpr = OptExpression.create(newAgg, leaves.get(i));
            } else {
                newAggExpr = OptExpression.create(newAgg, input.inputAt(0).inputAt(i));
            }

            newAggs.add(newAggExpr);

            List<ColumnRefOperator> columnRefOperators = new ArrayList<>();
            columnRefOperators.addAll(newAgg.getGroupingKeys());
            columnRefOperators.addAll(new ArrayList<>(originAgg.getAggregations().keySet()));

            unionInputs.add(columnRefOperators);
        }

        List<ColumnRefOperator> aggOutput = new ArrayList<>();
        aggOutput.addAll(originAgg.getGroupingKeys());
        aggOutput.addAll(new ArrayList<>(originAgg.getAggregations().keySet()));
        for (int i = 0; i < aggOutput.size(); i++) {
            if (originAgg.getAggregations().containsKey(aggOutput.get(i))) {
                aggOutput.get(i).setType(originAgg.getAggregations().get(aggOutput.get(i)).getType());
            }
        }
        LogicalUnionOperator newUnion = new LogicalUnionOperator(aggOutput, unionInputs, union.isUnionAll());
        return Lists.newArrayList(OptExpression.create(newUnion, newAggs));
    }

    public Map<ColumnRefOperator, CallOperator> createNormalAgg(Map<ColumnRefOperator, CallOperator> aggregationMap) {
        Map<ColumnRefOperator, CallOperator> newAggregationMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationMap.entrySet()) {
            ColumnRefOperator column = entry.getKey();
            CallOperator aggregation = entry.getValue();

            CallOperator callOperator = new CallOperator(aggregation.getFnName(), aggregation.getType(),
                    aggregation.getChildren(), aggregation.getFunction(),
                    aggregation.isDistinct(), aggregation.isRemovedDistinct());

            newAggregationMap.put(
                    new ColumnRefOperator(column.getId(), column.getType(), column.getName(), column.isNullable()),
                    callOperator);
        }

        return newAggregationMap;
    }

}
