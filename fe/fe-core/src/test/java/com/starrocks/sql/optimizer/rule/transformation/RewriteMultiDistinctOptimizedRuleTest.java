// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RewriteMultiDistinctOptimizedRuleTest {

    @Test
    public void transform() {
        RewriteMultiDistinctOptimizedRule rule = new RewriteMultiDistinctOptimizedRule();

        AggType type = AggType.GLOBAL;
        List<ColumnRefOperator> groupingKeys = new ArrayList<>();
        Map<ColumnRefOperator, CallOperator> aggregation = new HashMap<>();

        String fnName = "count";
        Type returnType = ScalarType.BIGINT;
        List<ScalarOperator> arguments = new ArrayList<>();

        List<Type> typeList = new ArrayList<>();
        typeList.add(ScalarType.VARCHAR);
        AggregateFunction aggregateFunction = new AggregateFunction(
                new FunctionName(fnName), typeList, ScalarType.BIGINT,
                null,false, true);
        arguments.add(new ColumnRefOperator(38, ScalarType.VARCHAR,"agent_name", true));
        aggregation.put(
                new ColumnRefOperator(46, ScalarType.BIGINT, "count", false),
                new CallOperator(fnName, returnType, arguments, aggregateFunction, true));

        List<ScalarOperator> arguments1 = new ArrayList<>();
        arguments1.add(new ColumnRefOperator(37, ScalarType.VARCHAR,"team_name", true));
        aggregation.put(
                new ColumnRefOperator(47, ScalarType.BIGINT, "count", false),
                new CallOperator(fnName, returnType, arguments1, aggregateFunction, true));

        List<ScalarOperator> arguments2 = new ArrayList<>();
        arguments2.add(new ColumnRefOperator(5, ScalarType.VARCHAR,"region_code", true));
        aggregation.put(
                new ColumnRefOperator(48, ScalarType.BIGINT, "count", false),
                new CallOperator(fnName, returnType, arguments2, aggregateFunction, true));

        LogicalAggregationOperator logicalAggregationOperator =
                new LogicalAggregationOperator(type, groupingKeys, aggregation);
        List<OptExpression> logicalAggregationOperatorInputs = new ArrayList<>();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
        columnRefMap.put(
                new ColumnRefOperator(37, ScalarType.VARCHAR,"team_name", true),
                new ColumnRefOperator(37, ScalarType.VARCHAR,"team_name", true));
        columnRefMap.put(
                new ColumnRefOperator(5, ScalarType.VARCHAR,"region_code", true),
                new ColumnRefOperator(5, ScalarType.VARCHAR,"region_code", true));
        columnRefMap.put(
                new ColumnRefOperator(38, ScalarType.VARCHAR,"agent_name", true),
                new ColumnRefOperator(38, ScalarType.VARCHAR,"agent_name", true));

        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(columnRefMap);
        List<OptExpression> projectInputs = new ArrayList<>();

        OptExpression optExpression = OptExpression.create(logicalProjectOperator, projectInputs);
        logicalAggregationOperatorInputs.add(optExpression);
        OptExpression input = OptExpression.create(logicalAggregationOperator, logicalAggregationOperatorInputs);
        OptimizerContext context = new OptimizerContext(new Memo(), new ColumnRefFactory());
        List<OptExpression> list = rule.transform(input, context);
        Operator operator = list.get(0).inputAt(0).getOp();
        List<OptExpression> inputs = list.get(0).inputAt(1).getInputs();
        assertEquals(OperatorType.LOGICAL_CTE_PRODUCE, operator.getOpType());
        assertEquals(2, inputs.size());
    }


}
