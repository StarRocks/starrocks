// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class MergeProjectWithChildRule extends TransformationRule {
    public MergeProjectWithChildRule() {
        super(RuleType.TF_MERGE_PROJECT_WITH_CHILD,
                Pattern.create(OperatorType.LOGICAL_PROJECT).
                        addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator logicalProjectOperator = (LogicalProjectOperator) input.getOp();
        Operator child = input.inputAt(0).getOp();

        Operator.Builder builder = OperatorBuilderFactory.build(child);
        Operator op = builder.withOperator(child).setProjection(new Projection(logicalProjectOperator.getColumnRefMap(),
                logicalProjectOperator.getCommonSubOperatorMap())).build();

        return Lists.newArrayList(OptExpression.create(op, input.inputAt(0).getInputs()));
    }

    public static class OperatorBuilderFactory {
        public static Operator.Builder<?, ?> build(Operator operator) {
            if (operator instanceof LogicalJoinOperator) {
                return new LogicalJoinOperator.Builder();
            } else if (operator instanceof LogicalAggregationOperator) {
                return new LogicalAggregationOperator.Builder();
            } else if (operator instanceof LogicalTopNOperator) {
                return new LogicalTopNOperator.Builder();
            } else if (operator instanceof LogicalOlapScanOperator) {
                return new LogicalOlapScanOperator.Builder();
            } else if (operator instanceof LogicalEsScanOperator) {
                return new LogicalEsScanOperator.Builder();
            } else if (operator instanceof LogicalMysqlScanOperator) {
                return new LogicalMysqlScanOperator.Builder();
            } else if (operator instanceof LogicalSchemaScanOperator) {
                return new LogicalSchemaScanOperator.Builder();
            } else if (operator instanceof LogicalValuesOperator) {
                return new LogicalValuesOperator.Builder();
            } else if (operator instanceof LogicalTableFunctionOperator) {
                return new LogicalTableFunctionOperator.Builder();
            } else if (operator instanceof LogicalWindowOperator) {
                return new LogicalWindowOperator.Builder();
            } else if (operator instanceof LogicalUnionOperator) {
                return new LogicalUnionOperator.Builder();
            } else if (operator instanceof LogicalExceptOperator) {
                return new LogicalExceptOperator.Builder();
            } else if (operator instanceof LogicalIntersectOperator) {
                return new LogicalIntersectOperator.Builder();
            } else if (operator instanceof LogicalFilterOperator) {
                return new LogicalFilterOperator.Builder();
            } else if (operator instanceof LogicalAssertOneRowOperator) {
                return new LogicalAssertOneRowOperator.Builder();
            } else {
                throw new StarRocksPlannerException("not implement builder", ErrorType.INTERNAL_ERROR);
            }
        }
    }
}
