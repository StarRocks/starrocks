// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.FunctionSet;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RewriteMultiDistinctOptimizedRule extends TransformationRule {


    public RewriteMultiDistinctOptimizedRule() {
        super(RuleType.TF_REWRITE_MULTI_DISTINCT_OPTIMIZED,
                        Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                                        OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // check cte is disable or hasNoGroup false
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        boolean hasNoGroup = agg.getGroupingKeys().size()  == 0 ? true : false;
        if (!context.getSessionVariable().isCboCteReuse() || !hasNoGroup) {
            return false;
        }

        List<CallOperator> distinctAggOperatorList = agg.getAggregations().values().stream()
                        .filter(CallOperator::isDistinct).collect(Collectors.toList());

        boolean hasMultiColumns = false;
        for (CallOperator callOperator : distinctAggOperatorList) {
            if (callOperator.getChildren().size() > 1) {
                hasMultiColumns = true;
                break;
            }
        }

        boolean isCheck = (distinctAggOperatorList.size() > 1
                && !hasMultiColumns) || agg.getAggregations().values().stream()
                .anyMatch(call -> call.isDistinct() && call.getFnName().equals(FunctionSet.AVG));
        if (isCheck) {
            context.getCteContext().setForcedCTE(true);
        }
        return isCheck;
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
        Map<ColumnRefOperator, CallOperator> countColumnMap = originalAllAggregationOperator.getAggregations();
        Map<String, ColumnRefOperator> columnMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : countColumnMap.entrySet()) {
            ColumnRefOperator crf = (ColumnRefOperator) entry.getValue().getChild(0);
            String columnName = crf.getName();
            columnMap.put(columnName, crf);
        }

        Map<ColumnRefOperator, CallOperator> copyCountColumnMap = new HashMap<>();
        copyCountColumnMap.putAll(countColumnMap);
        OptExpression inputs = constructJoinOperatorInputs(null, copyCountColumnMap, columnMap,
                columnRefFactory,
                originalAllAggregationOperator,
                cteId);

        anchorOperatorInputsList.add(inputs);

        List<OptExpression> result =
                Lists.newArrayList(OptExpression.create(anchorOperator, anchorOperatorInputsList));

        return result;
    }

    private OptExpression constructChildOptExpressions(Map.Entry<ColumnRefOperator, CallOperator> ent,
                                                       Map<String, ColumnRefOperator> columnMap,
                                                       ColumnRefFactory columnRefFactory,
                                                       LogicalAggregationOperator originalAllAggregationOperator,
                                                       Integer cteId) {
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

        return OptExpression.create(singleAggOperator, singleAggOperatorInputs);
    }


    private OptExpression constructJoinOperatorInputs(OptExpression optExpression,
                                                      Map<ColumnRefOperator, CallOperator> countColumnMap,
                                                      Map<String, ColumnRefOperator> columnMap,
                                                      ColumnRefFactory columnRefFactory,
                                                      LogicalAggregationOperator originalAllAggregationOperator,
                                                      Integer cteId) {
        Iterator<Map.Entry<ColumnRefOperator, CallOperator>> iter = countColumnMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<ColumnRefOperator, CallOperator> entry = iter.next();
            OptExpression childOptExpression =
                    constructChildOptExpressions(entry,
                            columnMap, columnRefFactory, originalAllAggregationOperator, cteId);
            if (optExpression == null) {
                iter.remove();
                return constructJoinOperatorInputs(childOptExpression,
                        countColumnMap,
                        columnMap,
                        columnRefFactory,
                        originalAllAggregationOperator,
                        cteId);
            }
            LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder()
                    .setJoinType(JoinOperator.CROSS_JOIN)
                    .setOnPredicate(null)
                    .build();
            List<OptExpression> joinOperatorInputsList = new ArrayList<>();
            joinOperatorInputsList.add(optExpression);
            joinOperatorInputsList.add(childOptExpression);
            OptExpression newOptExpression = OptExpression.create(
                    joinOperator, joinOperatorInputsList);
            iter.remove();
            return constructJoinOperatorInputs(newOptExpression,
                    countColumnMap,
                    columnMap,
                    columnRefFactory,
                    originalAllAggregationOperator,
                    cteId);
        }
        return optExpression;
    }

}
