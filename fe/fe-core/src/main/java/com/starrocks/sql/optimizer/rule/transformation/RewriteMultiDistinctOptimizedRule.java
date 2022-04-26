// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;


import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @author wangxiao060
 * @create 2022-04-10 20:56
 **/
public class RewriteMultiDistinctOptimizedRule extends TransformationRule {


    public RewriteMultiDistinctOptimizedRule() {
        super(RuleType.TF_REWRITE_MULTI_DISTINCT_OPTIMIZED,
                        Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                                        OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();

        List<CallOperator> distinctAggOperatorList = agg.getAggregations().values().stream()
                        .filter(CallOperator::isDistinct).collect(Collectors.toList());

        boolean hasMultiColumns = false;
        for (CallOperator callOperator : distinctAggOperatorList) {
            if (callOperator.getChildren().size() > 1) {
                hasMultiColumns = true;
                break;
            }
        }

        return (distinctAggOperatorList.size() > 1 && !hasMultiColumns) || agg.getAggregations().values().stream()
                        .anyMatch(call -> call.isDistinct());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        // define cteId
        Integer cteId = columnRefFactory.getNextRelationId();
        // build logic cte produce operator
        List<OptExpression> aggChildOptExpressionList = input.getInputs();
        LogicalCTEProduceOperator logicalCTEProduceOperator = new LogicalCTEProduceOperator(cteId);
        // build cte anchor operator
        LogicalCTEAnchorOperator anchorOperator = new LogicalCTEAnchorOperator(cteId);
        List<OptExpression> anchorOperatorInputsList = new ArrayList<>();
        anchorOperatorInputsList.add(OptExpression.create(logicalCTEProduceOperator, aggChildOptExpressionList));
        // get all aggregation operator
        LogicalAggregationOperator originalAllAggregationOperator = (LogicalAggregationOperator) input.getOp();

        // get metric method operator
        LogicalJoinOperator firstJoinOperator = new LogicalJoinOperator.Builder()
                .setJoinType(JoinOperator.CROSS_JOIN)
                .setOnPredicate(null)
                .build();
        List<OptExpression> fistJoinOperatorInputsList = new ArrayList<>();
        // group count divided into pairs
        Map<ColumnRefOperator, CallOperator> countColumnMap = originalAllAggregationOperator.getAggregations();
        Map<Integer, Map<ColumnRefOperator, CallOperator>> groupCountColumnMap = new HashMap<>();

        int index = 0;
        int num = 0;

        Map<String, ColumnRefOperator> columnMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : countColumnMap.entrySet()) {
            ColumnRefOperator crf = (ColumnRefOperator) entry.getValue().getChild(0);
            String columnName = crf.getName();
            columnMap.put(columnName, crf);
            if (index % 2 == 0) {
                Map<ColumnRefOperator, CallOperator> map = new HashMap<>();
                map.put(entry.getKey(), entry.getValue());
                groupCountColumnMap.put(num, map);
            } else {
                Map<ColumnRefOperator, CallOperator> map = groupCountColumnMap.get(num);
                map.put(entry.getKey(), entry.getValue());
                groupCountColumnMap.put(num, map);
                num = num + 1;
            }
            index = index + 1;
        }

        for (Map.Entry<Integer, Map<ColumnRefOperator, CallOperator>> entry : groupCountColumnMap.entrySet()) {
            OptExpression methodInputs = null;
            if (entry.getValue().size() < 2) {

                for (Map.Entry<ColumnRefOperator, CallOperator> ent : entry.getValue().entrySet()) {
                    CallOperator oldCallOperator = ent.getValue();
                    List<ScalarOperator> list = new ArrayList<>();

                    Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap = new HashMap<>();
                    for (Map.Entry<String, ColumnRefOperator> ent0 : columnMap.entrySet()) {
                        String name = ((ColumnRefOperator) oldCallOperator.getChild(0)).getName();
                        ColumnRefOperator newColumnRefOperator =
                                columnRefFactory.create(ent0.getValue().getName(),
                                        ent0.getValue().getType(), true);
                        if (name.equals(ent0.getKey())) {
                            list.add(newColumnRefOperator);
                        }
                        cteOutputColumnRefMap.put(newColumnRefOperator, ent0.getValue());
                    }

                    CallOperator newCallOperator = new CallOperator(
                            oldCallOperator.getFnName(), oldCallOperator.getType(),
                            list, oldCallOperator.getFunction(), oldCallOperator.isDistinct());

                    Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
                    aggregations.put(ent.getKey(), newCallOperator);
                    LogicalAggregationOperator singleAggOperator = new LogicalAggregationOperator(
                            originalAllAggregationOperator.getType(),
                            originalAllAggregationOperator.getGroupingKeys(),
                            aggregations);

                    LogicalCTEConsumeOperator cteConsumeOperator = new LogicalCTEConsumeOperator(cteId, cteOutputColumnRefMap);
                    OptExpression singleAggOperatorInputs = new OptExpression(cteConsumeOperator);

                    methodInputs = OptExpression.create(singleAggOperator, singleAggOperatorInputs);

                }
            } else {
                // Join -> project 1. count 2.count ; 1.count -> agg -> project metric column -> cte consumer
                LogicalJoinOperator secondJoinOperator = new LogicalJoinOperator.Builder()
                        .setJoinType(JoinOperator.CROSS_JOIN)
                        .setOnPredicate(null)
                        .build();
                List<OptExpression> secondJoinOperatorInputsList = new ArrayList<>();

                for (Map.Entry<ColumnRefOperator, CallOperator> ent : entry.getValue().entrySet()) {
                    CallOperator oldCallOperator = ent.getValue();
                    List<ScalarOperator> list = new ArrayList<>();

                    Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap = new HashMap<>();
                    for (Map.Entry<String, ColumnRefOperator> ent0 : columnMap.entrySet()) {
                        String name = ((ColumnRefOperator) oldCallOperator.getChild(0)).getName();
                        ColumnRefOperator newColumnRefOperator =
                                columnRefFactory.create(ent0.getValue().getName(),
                                        ent0.getValue().getType(), true);
                        if (name.equals(ent0.getKey())) {
                            list.add(newColumnRefOperator);
                        }
                        cteOutputColumnRefMap.put(newColumnRefOperator, ent0.getValue());
                    }

                    CallOperator newCallOperator = new CallOperator(
                            oldCallOperator.getFnName(), oldCallOperator.getType(),
                            list, oldCallOperator.getFunction(), oldCallOperator.isDistinct());

                    Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
                    aggregations.put(ent.getKey(), newCallOperator);
                    LogicalAggregationOperator singleAggOperator = new LogicalAggregationOperator(
                            originalAllAggregationOperator.getType(),
                            originalAllAggregationOperator.getGroupingKeys(),
                            aggregations);

                    LogicalCTEConsumeOperator cteConsumeOperator = new LogicalCTEConsumeOperator(cteId, cteOutputColumnRefMap);
                    OptExpression singleAggOperatorInputs = new OptExpression(cteConsumeOperator);

                    OptExpression optExpression = OptExpression.create(singleAggOperator, singleAggOperatorInputs);
                    secondJoinOperatorInputsList.add(optExpression);
                }
                methodInputs = OptExpression.create(
                        secondJoinOperator, secondJoinOperatorInputsList);
            }
            fistJoinOperatorInputsList.add(methodInputs);
        }

        anchorOperatorInputsList.add(OptExpression.create(firstJoinOperator, fistJoinOperatorInputsList));

        context.getSessionVariable().setCboCteReuse(true);
        context.getCteContext().setEnableCTE(true);
        List<OptExpression> result =
                Lists.newArrayList(OptExpression.create(anchorOperator, anchorOperatorInputsList));

        return result;
    }

}
