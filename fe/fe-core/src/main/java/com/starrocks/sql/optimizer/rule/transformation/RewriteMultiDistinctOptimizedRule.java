// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;


import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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
                        .anyMatch(call -> call.isDistinct() && call.getFnName().equals(FunctionSet.AVG));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        LinkedList<Group> groupList = (LinkedList<Group>) context.getMemo().getGroups();
        // get olap scan operator
        Group firstGroup = groupList.getFirst();
        List<GroupExpression> scanGroupExpressionList = firstGroup.getLogicalExpressions();
        LogicalOlapScanOperator originalOlapScanOperator =
                (LogicalOlapScanOperator) scanGroupExpressionList.get(0).getOp();
        Map<Column, ColumnRefOperator> mapAllColumn = originalOlapScanOperator.getColumnMetaToColRefMap();
        OptExpression originalAllColumnsProjectOperatorInputs = new OptExpression(originalOlapScanOperator);
        // get all columns project operator
        Group secondGroup = groupList.get(1);
        List<GroupExpression>  allColumnsProjectExpressionList = secondGroup.getLogicalExpressions();
        LogicalProjectOperator originalAllColumnsProjectOperator =
                (LogicalProjectOperator) allColumnsProjectExpressionList.get(0).getOp();
        OptExpression projectOperatorInputs = OptExpression.create(
                originalAllColumnsProjectOperator,
                originalAllColumnsProjectOperatorInputs);

        // get metric columns project operator
        Group thirdGroup = groupList.get(2);
        List<GroupExpression>  projectGroupExpressionList = thirdGroup.getLogicalExpressions();
        LogicalProjectOperator originalAllMetricsProjectOperator =
                (LogicalProjectOperator) projectGroupExpressionList.get(0).getOp();
        // define cteId
        Integer cteId = 122;
        // build logic cte produce operator
        LogicalCTEProduceOperator logicalCTEProduceOperator = new LogicalCTEProduceOperator(cteId);
        OptExpression logicalCTEProduceOperatorInputs =
                OptExpression.create(originalAllMetricsProjectOperator, projectOperatorInputs);

        // build cte anchor operator
        LogicalCTEAnchorOperator anchorOperator = new LogicalCTEAnchorOperator(cteId);
        List<OptExpression> anchorOperatorInputsList = new ArrayList<>();
        anchorOperatorInputsList.add(OptExpression.create(logicalCTEProduceOperator, logicalCTEProduceOperatorInputs));

        // get all aggregation operator
        Group fourthGroup = groupList.get(3);
        LogicalAggregationOperator originalAllAggregationOperator = (LogicalAggregationOperator)
                fourthGroup.getLogicalExpressions().get(0).getOp();

        // get metric method operator
        Group lastGroup = groupList.getLast();
        LogicalProjectOperator firstProjectOperator =
                (LogicalProjectOperator) lastGroup.getLogicalExpressions().get(0).getOp();
        OptExpression firstProjectOperatorInputs = null;
        LogicalJoinOperator firstJoinOperator = new LogicalJoinOperator.Builder()
                .setJoinType(JoinOperator.CROSS_JOIN)
                .setOnPredicate(null)
                .build();
        List<OptExpression> fistJoinOperatorInputsList = new ArrayList<>();

        // group count divided into pairs
        Map<ColumnRefOperator, ScalarOperator> countColumnMap = firstProjectOperator.getColumnRefMap();
        Map<Integer, Map<ColumnRefOperator, ScalarOperator>> groupCountColumnMap = new HashMap<>();

        int index = 0;
        int num = 0;
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : countColumnMap.entrySet()) {
            if (index % 2 == 0) {
                Map<ColumnRefOperator, ScalarOperator> map = new HashMap<>();
                map.put(entry.getKey(), entry.getValue());
                groupCountColumnMap.put(num, map);
            } else {
                Map<ColumnRefOperator, ScalarOperator> map = groupCountColumnMap.get(num);
                map.put(entry.getKey(), entry.getValue());
                groupCountColumnMap.put(num, map);
                num = num + 1;
            }
            index = index + 1;
        }

        for (Map.Entry<Integer, Map<ColumnRefOperator, ScalarOperator>> entry : groupCountColumnMap.entrySet()) {
            LogicalProjectOperator methodLogicalProjectOperator = new LogicalProjectOperator(entry.getValue());
            List<OptExpression> countFirstProjectionlist = new ArrayList<>();
            OptExpression methodLogicalProjectOperatorInputs = null;
            if (entry.getValue().size() < 2) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> ent : entry.getValue().entrySet()) {
                    methodLogicalProjectOperatorInputs = constructChildTree(ent,
                            originalOlapScanOperator,
                            originalAllAggregationOperator,
                            originalAllMetricsProjectOperator,
                            columnRefFactory,
                            cteId);
                }
            } else {
                LogicalJoinOperator secondJoinOperator = new LogicalJoinOperator.Builder()
                        .setJoinType(JoinOperator.CROSS_JOIN)
                        .setOnPredicate(null)
                        .build();
                List<OptExpression> secondJoinOperatorInputsList = new ArrayList<>();
                for (Map.Entry<ColumnRefOperator, ScalarOperator> ent : entry.getValue().entrySet()) {
                    OptExpression secondJoinOperatorInputs = constructChildTree(ent,
                            originalOlapScanOperator,
                            originalAllAggregationOperator,
                            originalAllMetricsProjectOperator,
                            columnRefFactory,
                            cteId);
                    secondJoinOperatorInputsList.add(secondJoinOperatorInputs);
                }
                methodLogicalProjectOperatorInputs = OptExpression.create(
                        secondJoinOperator, secondJoinOperatorInputsList);


            }
            countFirstProjectionlist.add(methodLogicalProjectOperatorInputs);
            fistJoinOperatorInputsList.add(OptExpression.create(methodLogicalProjectOperator, countFirstProjectionlist));
            firstProjectOperatorInputs = OptExpression.create(firstJoinOperator, fistJoinOperatorInputsList);
        }

        anchorOperatorInputsList.add(OptExpression.create(
                firstProjectOperator,
                OptExpression.create(firstProjectOperator,
                        firstProjectOperatorInputs)));

        context.getSessionVariable().setCboCteReuse(true);
        context.getCteContext().setEnableCTE(true);
        List<OptExpression> result =
                Lists.newArrayList(OptExpression.create(anchorOperator, anchorOperatorInputsList));

        return result;
    }


    private OptExpression constructChildTree(Map.Entry<ColumnRefOperator, ScalarOperator> ent,
                                             LogicalOlapScanOperator originalOlapScanOperator,
                                             LogicalAggregationOperator originalAllAggregationOperator,
                                             LogicalProjectOperator originalAllMetricsProjectOperator,
                                             ColumnRefFactory columnRefFactory,
                                             int cteId) {
        Map<ColumnRefOperator, ScalarOperator> map = new HashMap<>();
        map.put(ent.getKey(), ent.getValue());
        LogicalProjectOperator singleCountProjectOperator = new LogicalProjectOperator(map);
        Map<Column, ColumnRefOperator> mapAllColumn = originalOlapScanOperator.getColumnMetaToColRefMap();
        Map<String, ColumnRefOperator> newAllColumnMap = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> newAllColumnCSMap = new HashMap<>();
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
        Table table = originalOlapScanOperator.getTable();
        for (Map.Entry<Column, ColumnRefOperator> columnEntry : mapAllColumn.entrySet()) {
            String columnName = columnEntry.getKey().getName();
            ColumnRefOperator columnRefOperator = columnEntry.getValue();
            ColumnRefOperator newColumnRefOperator =
                    columnRefFactory.create(columnRefOperator.getName(), columnRefOperator.getType(), true);
            newAllColumnMap.put(columnName, newColumnRefOperator);
            newAllColumnCSMap.put(newColumnRefOperator, newColumnRefOperator);
            colRefToColumnMetaMap.put(newColumnRefOperator, columnEntry.getKey());
            columnMetaToColRefMap.put(columnEntry.getKey(), newColumnRefOperator);
        }

        Map<ColumnRefOperator, CallOperator> aggColumns = originalAllAggregationOperator.getAggregations();
        CallOperator callOperator = aggColumns.get(ent.getKey());
        ColumnRefOperator newColumnRefOperator =
                newAllColumnMap.get(((ColumnRefOperator) callOperator.getChild(0)).getName());
        List<ScalarOperator> list = new ArrayList<>();
        list.add(newColumnRefOperator);
        CallOperator newCallOperator = new CallOperator(
                callOperator.getFnName(), callOperator.getType(),
                list, callOperator.getFunction(), callOperator.isDistinct());

        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        aggregations.put(ent.getKey(), newCallOperator);
        LogicalAggregationOperator logicalAggregationOperator = new LogicalAggregationOperator(
                originalAllAggregationOperator.getType(),
                originalAllAggregationOperator.getGroupingKeys(),
                aggregations);

        Map<ColumnRefOperator, ScalarOperator> singleMetricColumnMap = new HashMap<>();
        singleMetricColumnMap.put(newColumnRefOperator, newColumnRefOperator);
        LogicalProjectOperator singleMetricColumnlogicalProjectOperator
                = new LogicalProjectOperator(singleMetricColumnMap);

        Map<ColumnRefOperator, ColumnRefOperator> cteOutputColumnRefMap = new HashMap<>();

        Map<ColumnRefOperator, ScalarOperator> originalMetricColumnMap =
                originalAllMetricsProjectOperator.getColumnRefMap();
        Map<ColumnRefOperator, ScalarOperator> allMetricColumnMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> metricColumnEntry :
                originalMetricColumnMap.entrySet()) {
            String columnName = metricColumnEntry.getKey().getName();
            ColumnRefOperator originalMetricColumnRefOperator = (ColumnRefOperator) metricColumnEntry.getValue();
            ColumnRefOperator newMetricColumnRefOperator = newAllColumnMap.get(columnName);
            cteOutputColumnRefMap.put(newMetricColumnRefOperator, originalMetricColumnRefOperator);
            allMetricColumnMap.put(newMetricColumnRefOperator, newMetricColumnRefOperator);
        }

        LogicalCTEConsumeOperator logicalCTEConsumeOperator =
                new LogicalCTEConsumeOperator(cteId, cteOutputColumnRefMap);

        LogicalProjectOperator allMetricColumnProjectOperator = new LogicalProjectOperator(allMetricColumnMap);

        LogicalProjectOperator allColumnProjectOperator = new LogicalProjectOperator(newAllColumnCSMap);

        LogicalOlapScanOperator newOlapScanOperator = new LogicalOlapScanOperator(
                table, colRefToColumnMetaMap, columnMetaToColRefMap, null,
                originalOlapScanOperator.getLimit(), null);
        OptExpression allColumnProjectOperatorInputs = new OptExpression(newOlapScanOperator);

        OptExpression allMetricColumnProjectOperatorInputs = OptExpression.create(allColumnProjectOperator,
                allColumnProjectOperatorInputs);
        OptExpression logicalCTEConsumeOperatorInputs = OptExpression.create(allMetricColumnProjectOperator,
                allMetricColumnProjectOperatorInputs);

        OptExpression singleMetricColumnlogicalProjectOperatorInputs = OptExpression.create(
                logicalCTEConsumeOperator,
                logicalCTEConsumeOperatorInputs);

        OptExpression logicalAggregationOperatorInputs = OptExpression.create(
                singleMetricColumnlogicalProjectOperator,
                singleMetricColumnlogicalProjectOperatorInputs);

        OptExpression singleCountProjectOperatorInputs = OptExpression.create(logicalAggregationOperator,
                logicalAggregationOperatorInputs);

        OptExpression methodLogicalProjectOperatorInputs = OptExpression.create(singleCountProjectOperator,
                singleCountProjectOperatorInputs);

        return methodLogicalProjectOperatorInputs;
    }

}
